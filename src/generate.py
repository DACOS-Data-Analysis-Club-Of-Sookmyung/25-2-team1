from __future__ import annotations

import asyncio
import importlib
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.llm.client import OpenAICompatClient


# ----------------
# helpers
# ----------------
def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def render_prompt(prompt_md: str, ctx: Dict[str, Any]) -> str:
    # NOTE: prompt.md 안에서 {corp_name} 같은 키를 쓰는 방식 유지
    return prompt_md.format(**ctx)


def ensure_workdir_layout(workdir: Path) -> None:
    """
    팀 규칙:
    - meta.json은 workdir/meta/meta.json
    - metrics는 workdir/metrics/
    - evidence는 workdir/evidence/
    - summary는 workdir/summary/
    """
    (workdir / "meta").mkdir(parents=True, exist_ok=True)
    (workdir / "metrics").mkdir(parents=True, exist_ok=True)
    (workdir / "evidence").mkdir(parents=True, exist_ok=True)
    (workdir / "summary").mkdir(parents=True, exist_ok=True)


def build_inputs_via_script(
    workdir_root: Path,
    section_dir: Path,
    company: str,
    year: int,
    topk: int = 5,
    db_path: Path | None = None,
) -> Path:
    SECTIONS_ROOT = (Path(__file__).resolve().parent / "sections").resolve()
    rel = section_dir.resolve().relative_to(SECTIONS_ROOT)  # c01_intro/s01_1_objective
    section_arg = "/".join(rel.parts)

    cmd = [
        "python", "scripts/run_section.py",
        "--company", company,
        "--year", str(year),
        "--section", section_arg,
        "--build",
        "--workdir", str(workdir_root),
        "--topk", str(topk),
    ]
    if db_path is not None:
        cmd += ["--db-path", str(db_path)]

    subprocess.run(cmd, check=True)

    return workdir_root



def run_section(
    workdir_root: Path,
    section_dir: Path,
    client: OpenAICompatClient,
    system_rules: str,
    out_dir: Path,
    *,
    company: str,
    year: int,
    topk: int = 5,
    db_path: Path | None = None,
    build_inputs: bool = True,
) -> Path:
    workdir_root = workdir_root.resolve()
    section_dir = section_dir.resolve()

    # 0) inputs 생성 → section_workdir(out_base)
    if build_inputs:
        build_inputs_via_script(
            workdir_root=workdir_root,
            section_dir=section_dir,
            company=company,
            year=year,
            topk=topk,
            db_path=db_path,
        )


    # 1) spec/prompt
    spec = read_json(section_dir / "inputs_spec.json")
    prompt_md = read_text(section_dir / spec["prompt"])

    # 2) retriever import
    SECTIONS_ROOT = (Path(__file__).resolve().parent / "sections").resolve()
    rel = section_dir.relative_to(SECTIONS_ROOT)
    pkg = "src.sections." + ".".join(rel.parts)

    retr = importlib.import_module(pkg + ".retriever")
    if not hasattr(retr, "build_ctx"):
        raise AttributeError(f"{pkg}.retriever 에 build_ctx(workdir, spec) 함수가 없습니다.")

    # ✅ 핵심: workdir_root가 아니라 section_workdir(out_base)를 넘김
    ctx = retr.build_ctx(workdir=workdir_root, spec=spec)
    user_prompt = render_prompt(prompt_md, ctx)

    # 3) LLM 호출
    llm = spec.get("llm", {})
    raw = client.chat(
        system=system_rules,
        user=user_prompt,
        temperature=llm.get("temperature", 0.2),
        max_tokens=llm.get("max_tokens", 1600),
    )

    # 4) 저장
    out = {"id": spec["id"], "section_id": spec["section_id"], "title": spec["title"], "content": raw}
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{spec['id']}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


async def run_sections_parallel(
    workdir_root: Path,
    section_dirs: List[Path],
    client: OpenAICompatClient,
    system_rules: str,
    out_dir: Path,
    *,
    company: str,
    year: int,
    topk: int = 5,
    db_path: Path | None = None,
    build_inputs: bool = True,
    build_inputs_sequential: bool = True,
) -> List[Path]:
    workdir_root = workdir_root.resolve()

    if build_inputs and build_inputs_sequential:
        for sdir in section_dirs:
            build_inputs_via_script(
                workdir_root=workdir_root,
                section_dir=sdir.resolve(),
                company=company,
                year=year,
                topk=topk,
                db_path=db_path,
            )

        loop = asyncio.get_running_loop()
        tasks = [
            loop.run_in_executor(
                None,
                lambda sd=sdir: run_section(
                    workdir_root=workdir_root,
                    section_dir=sd,
                    client=client,
                    system_rules=system_rules,
                    out_dir=out_dir,
                    company=company,
                    year=year,
                    topk=topk,
                    db_path=db_path,
                    build_inputs=False,
                ),
            )
            for sdir in section_dirs
        ]
        return await asyncio.gather(*tasks)

    loop = asyncio.get_running_loop()
    tasks = [
        loop.run_in_executor(
            None,
            lambda sd=sdir: run_section(
                workdir_root=workdir_root,
                section_dir=sd,
                client=client,
                system_rules=system_rules,
                out_dir=out_dir,
                company=company,
                year=year,
                topk=topk,
                db_path=db_path,
                build_inputs=build_inputs,
            ),
        )
        for sdir in section_dirs
    ]
    return await asyncio.gather(*tasks)

# ----------------
# bridge summary
# ----------------
def _parse_chapter_no(section_id: Optional[str]) -> Optional[int]:
    """section_id가 '3.2' 같은 형태라고 가정하고 장 번호(3)만 추출."""
    if not section_id:
        return None
    try:
        return int(str(section_id).split(".", 1)[0])
    except Exception:
        return None


def make_bridge_summary(
    client: OpenAICompatClient,
    out_dir: Path,
    workdir: Path,
    phase1_spec_ids: List[str],
    chapter_groups: List[Tuple[int, ...]] = [(1, 2), (3, 4, 5), (6, 7, 8)],
    max_chars_per_section: int = 1200,
    max_tokens: int = 1200,
) -> Path:
    """
    - 섹션 결과(out_dir/<spec_id>.json)를 읽어온 뒤
    - chapter_groups 단위로 LLM을 호출하여 섹션별 1~2문장 요약 생성
    - 결과를 workdir/summary/bridge_summary.json에 'chapters' 구조로 누적 저장
    - 마지막에 chapters를 기반으로 bridge_text를 "재생성"하여 저장
    """
    workdir = workdir.resolve()
    ensure_workdir_layout(workdir)

    # 0) 섹션 결과 수집
    items: List[Dict[str, Any]] = []
    for sid in phase1_spec_ids:
        fp = out_dir / f"{sid}.json"
        if not fp.exists():
            continue
        obj = read_json(fp)

        section_id = obj.get("section_id")
        chapter_no = _parse_chapter_no(section_id)
        if chapter_no is None:
            continue

        content = (obj.get("content") or "").strip().replace("\n\n", "\n")
        if len(content) > max_chars_per_section:
            content = content[:max_chars_per_section] + "…"

        items.append(
            {
                "chapter_no": chapter_no,
                "section_id": section_id,
                "title": obj.get("title"),
                "content": content,
            }
        )

    if not items:
        raise RuntimeError("Bridge 입력 섹션 결과가 비어있습니다. phase1 실행/출력 경로를 확인하세요.")

    items.sort(key=lambda x: (x["chapter_no"], str(x["section_id"])))

    # 1) LLM 호출(그룹 단위)
    system = (
        "너는 보고서 섹션 요약기다. "
        "각 섹션의 핵심 결론을 1~2문장으로 요약한다. "
        "새로운 숫자 생성 금지. 원문에 없는 사실 추가 금지. "
        "출력은 반드시 JSON만."
    )

    def _call_group(group_items: List[Dict[str, Any]], label: str) -> Dict[str, Any]:
        user = (
            "아래는 보고서 섹션별 생성 결과다.\n"
            "각 항목마다 1~2문장 요약을 만들어라.\n\n"
            "출력 JSON 스키마:\n"
            "{\n"
            '  "section_summaries": {"<section_id>": "<요약>", ...},\n'
            '  "group_text": "[GROUP SUMMARY]\\n- <section_id> <title>: ...\\n..."\n'
            "}\n\n"
            "입력:\n"
            f"{json.dumps(group_items, ensure_ascii=False, indent=2)}"
        )
        raw = client.chat(system=system, user=user, temperature=0.2, max_tokens=max_tokens)
        return json.loads(raw)

    # 2) 저장 객체 준비(기존 파일 있으면 이어쓰기)
    out_path = workdir / "summary" / "bridge_summary.json"
    if out_path.exists():
        try:
            bridge_obj = json.loads(out_path.read_text(encoding="utf-8"))
        except Exception:
            bridge_obj = {}
    else:
        bridge_obj = {}

    if "chapters" not in bridge_obj or not isinstance(bridge_obj["chapters"], dict):
        bridge_obj["chapters"] = {}

    # 3) 그룹별 호출 → 결과를 '장별'로 분배하여 저장
    for group in chapter_groups:
        group_set = set(group)
        group_items = [it for it in items if it["chapter_no"] in group_set]
        if not group_items:
            continue

        label = f"{min(group)}-{max(group)}" if len(group) > 1 else str(group[0])
        group_result = _call_group(group_items, label=label)

        section_summaries: Dict[str, str] = group_result.get("section_summaries", {}) or {}

        # 3-1) 섹션 요약을 장별로 나눠 담기
        by_chapter: Dict[int, Dict[str, str]] = {}
        for sec_id, summ in section_summaries.items():
            ch = _parse_chapter_no(sec_id)
            if ch is None:
                continue
            by_chapter.setdefault(ch, {})[sec_id] = summ

        # 3-2) 장별 chapter_text도 만들어 저장
        for ch, sec_map in by_chapter.items():
            ch_key = str(ch)
            if ch_key not in bridge_obj["chapters"]:
                bridge_obj["chapters"][ch_key] = {"section_summaries": {}, "chapter_text": ""}

            # 섹션 요약 merge
            bridge_obj["chapters"][ch_key]["section_summaries"].update(sec_map)

            # chapter_text 재생성(해당 장의 섹션만)
            lines = [f"[CHAPTER SUMMARY {ch}]"]
            for sec_id in sorted(
                sec_map.keys(),
                key=lambda x: [int(p) if p.isdigit() else p for p in str(x).split(".")]
            ):
                title = next((it["title"] for it in group_items if it["section_id"] == sec_id), "")
                lines.append(f"- {sec_id} {title}: {sec_map[sec_id]}")
            bridge_obj["chapters"][ch_key]["chapter_text"] = "\n".join(lines)

    # 4) chapters 기반으로 전체 bridge_text "재생성"
    ch_nums = sorted([int(k) for k in bridge_obj["chapters"].keys() if str(k).isdigit()])
    if ch_nums:
        max_ch = max(ch_nums)
        bridge_lines = [f"[BRIDGE SUMMARY 1-{max_ch}]"]
        for ch in ch_nums:
            txt = (bridge_obj["chapters"].get(str(ch), {}) or {}).get("chapter_text", "")
            if txt:
                bridge_lines.append(txt)
        bridge_obj["bridge_text"] = "\n\n".join(bridge_lines).strip()
    else:
        bridge_obj["bridge_text"] = ""

    # 5) 저장
    out_path.write_text(json.dumps(bridge_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path

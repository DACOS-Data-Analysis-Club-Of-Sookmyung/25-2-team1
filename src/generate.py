# src/generate.py
from __future__ import annotations

import asyncio
import importlib
import json
from pathlib import Path
from typing import Any, Dict, List

from src.llm.client import OpenAICompatClient


# --------
# helpers
# --------
def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8")


def read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def render_prompt(prompt_md: str, ctx: Dict[str, Any]) -> str:
    # NOTE: prompt.md 안에서 {corp_name} 같은 키를 쓰는 방식 유지
    return prompt_md.format(**ctx)


# --------
# core
# --------
def run_section(
    workdir: Path,
    section_dir: Path,
    client: OpenAICompatClient,
    system_rules: str,
    out_dir: Path,
) -> Path:
    """
    - 섹션 폴더 내 inputs_spec.json / prompt.md / retriever.py 를 사용
    - retriever.py 내 build_ctx(workdir, spec)로 ctx 생성
    - 결과를 outputs/sections/<spec_id>.json 으로 저장
    """
    spec = read_json(section_dir / "inputs_spec.json")
    prompt_md = read_text(section_dir / spec["prompt"])

    # build ctx: src/sections 이하 상대경로로 모듈 경로 생성 (중첩 폴더 대응)
    SECTIONS_ROOT = (Path(__file__).resolve().parent / "sections").resolve()
    section_dir = section_dir.resolve()
    try:
        rel = section_dir.relative_to(SECTIONS_ROOT)
    except ValueError as e:
        raise ValueError(f"section_dir({section_dir})가 SECTIONS_ROOT({SECTIONS_ROOT}) 하위가 아닙니다.") from e
    pkg = "src.sections." + ".".join(rel.parts)            # e.g. src.sections.c09_swot.s09_1_strength

    retr = importlib.import_module(pkg + ".retriever")
    if not hasattr(retr, "build_ctx"):
        raise AttributeError(f"{pkg}.retriever 에 build_ctx(workdir, spec) 함수가 없습니다.")

    ctx = retr.build_ctx(workdir=workdir, spec=spec)
    user_prompt = render_prompt(prompt_md, ctx)

    llm = spec.get("llm", {})
    raw = client.chat(
        system=system_rules,
        user=user_prompt,
        temperature=llm.get("temperature", 0.2),
        max_tokens=llm.get("max_tokens", 1600),
    )

    out = {
        "id": spec["id"],
        "section_id": spec["section_id"],
        "title": spec["title"],
        "content": raw,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{spec['id']}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


async def run_sections_parallel(
    workdir: Path,
    section_dirs: List[Path],
    client: OpenAICompatClient,
    system_rules: str,
    out_dir: Path,
) -> List[Path]:
    """
    병렬 실행: thread executor로 run_section 호출
    """
    loop = asyncio.get_running_loop()
    tasks = [
        loop.run_in_executor(None, run_section, workdir, sdir, client, system_rules, out_dir)
        for sdir in section_dirs
    ]
    return await asyncio.gather(*tasks)


def make_bridge_summary(
    client: OpenAICompatClient,
    out_dir: Path,
    workdir: Path,
    phase1_spec_ids: List[str],
    max_chars_per_section: int = 1200,
) -> Path:
    """
    1~8 섹션 결과(out_dir/<spec_id>.json)를 읽어 bridge_summary.json 생성
    - 모델 출력은 JSON만 요구
    """
    items: List[Dict[str, Any]] = []
    for sid in phase1_spec_ids:
        fp = out_dir / f"{sid}.json"
        if not fp.exists():
            continue
        obj = read_json(fp)
        content = (obj.get("content") or "").strip().replace("\n\n", "\n")
        if len(content) > max_chars_per_section:
            content = content[:max_chars_per_section] + "…"
        items.append(
            {
                "section_id": obj.get("section_id"),
                "title": obj.get("title"),
                "content": content,
            }
        )
    if not items:
        raise RuntimeError("Bridge 입력 섹션 결과가 비어있습니다. phase1 실행/출력 경로를 확인하세요.")

    system = (
        "너는 보고서 섹션 요약기다. "
        "각 섹션의 핵심 결론을 1~2문장으로 요약한다. "
        "새로운 숫자 생성 금지. 원문에 없는 사실 추가 금지. "
        "출력은 반드시 JSON만."
    )

    user = (
        "아래는 보고서 1~8장 섹션별 생성 결과다.\n"
        "각 항목마다 1~2문장 요약을 만들어라.\n\n"
        "출력 JSON 스키마:\n"
        "{\n"
        '  "section_summaries": {"<section_id>": "<요약>", ...},\n'
        '  "bridge_text": "[BRIDGE SUMMARY]\\n- <section_id> <title>: ...\\n..."\n'
        "}\n\n"
        "입력:\n"
        f"{json.dumps(items, ensure_ascii=False, indent=2)}"
    )

    raw = client.chat(system=system, user=user, temperature=0.2, max_tokens=1200)

    # JSON only를 강제했지만, 혹시 모델이 실수하면 여기서 터짐 -> 그게 오히려 빨리 원인 파악 가능
    bridge_obj = json.loads(raw)

    out_path = workdir / "bridge_summary.json"
    out_path.write_text(json.dumps(bridge_obj, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path

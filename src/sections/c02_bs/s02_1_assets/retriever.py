# app.py
import sys
import os
import json
import asyncio
from pathlib import Path

# --- path fix (repo 루트 기준) ---
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))           # 'src' 패키지 인식용
# --------------------------------

import streamlit as st

from src.generate import run_sections_parallel
from src.llm.client import OpenAICompatClient


def load_system_rules() -> str:
    return (ROOT / "prompts" / "common" / "system_rules.md").read_text(encoding="utf-8")


def make_client() -> OpenAICompatClient:
    base_url = os.getenv("LLM_BASE_URL", "http://localhost:8000/v1")
    model = os.getenv("LLM_MODEL", "kakaocorp/kanana-1.5-8b-instruct-2505")
    api_key = os.getenv("LLM_API_KEY", "")  # 로컬 vLLM이면 보통 빈 문자열
    return OpenAICompatClient(base_url=base_url, api_key=api_key, model=model)


def discover_section_dirs() -> list[Path]:
    sections_root = ROOT / "src" / "sections"
    dirs = []
    for p in sections_root.rglob("inputs_spec.json"):
        dirs.append(p.parent)
    # id 기준 정렬(있으면)
    def _key(d: Path):
        try:
            obj = json.loads((d / "inputs_spec.json").read_text(encoding="utf-8"))
            return obj.get("id", str(d))
        except Exception:
            return str(d)
    return sorted(dirs, key=_key)


st.set_page_config(page_title="DART 재무 리포트 생성 데모", layout="centered")
st.title("DART 재무 리포트 생성 데모")

# ✅ 입력은 corp_name, bsns_year만
corp_name = st.text_input("기업명 (corp_name)", value="")
bsns_year = st.number_input("연도 (bsns_year)", value=2024, step=1)

# 옵션(데모 안정성/속도용)
topk = st.slider("근거 topk", min_value=1, max_value=15, value=5, step=1)

all_section_dirs = discover_section_dirs()

with st.expander("실행할 섹션 선택", expanded=False):
    # 기본은 전체
    selected = st.multiselect(
        "섹션",
        options=all_section_dirs,
        default=all_section_dirs,
        format_func=lambda d: d.relative_to(ROOT).as_posix(),
    )

with st.expander("현재 LLM 설정 보기", expanded=False):
    st.write("LLM_BASE_URL:", os.getenv("LLM_BASE_URL", "http://localhost:8000/v1"))
    st.write("LLM_MODEL:", os.getenv("LLM_MODEL", "kakaocorp/kanana-1.5-8b-instruct-2505"))
    st.write("LLM_API_KEY:", "(empty)" if not os.getenv("LLM_API_KEY") else "(set)")

if st.button("리포트 생성"):
    corp_name = corp_name.strip()
    if not corp_name:
        st.error("기업명을 입력해 주세요.")
        st.stop()

    if not selected:
        st.error("최소 1개 섹션은 선택해야 합니다.")
        st.stop()

    try:
        client = make_client()
        system_rules = load_system_rules()

        # ✅ workdir_root: generate.py가 workdir_root/company/year/... 하위로 만들어 씀
        workdir_root = ROOT / "workdir"

        # ✅ 출력 폴더를 회사/연도별로 분리(덮어쓰기 방지)
        out_dir = ROOT / "outputs" / "sections" / corp_name / str(int(bsns_year))
        out_dir.mkdir(parents=True, exist_ok=True)

        with st.spinner("입력 생성 + 섹션 병렬 실행 중..."):
            # generate.run_sections_parallel 은 async 함수라 asyncio.run 필요
            out_paths = asyncio.run(
                run_sections_parallel(
                    workdir_root=workdir_root,
                    section_dirs=selected,
                    client=client,
                    system_rules=system_rules,
                    out_dir=out_dir,
                    company=corp_name,
                    year=int(bsns_year),
                    topk=int(topk),
                    build_inputs=True,               # scripts/run_section.py --build 실행
                    build_inputs_sequential=True,    # 입력 생성은 순차(안정)
                )
            )

        st.success("완료!")
        st.write("출력 경로:", str(out_dir))

        # 결과 미리보기(가벼운 데모용)
        st.subheader("생성된 섹션")
        for p in out_paths:
            try:
                obj = json.loads(Path(p).read_text(encoding="utf-8"))
                with st.expander(f"{obj.get('id')} | {obj.get('title')}"):
                    st.write(obj.get("content", ""))
            except Exception:
                st.write(str(p))

    except Exception as e:
        st.error(f"실행 중 오류: {e}")

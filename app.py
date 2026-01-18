import sys
import os
import asyncio
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import streamlit as st

from src.generate import run_sections_parallel
from src.llm.client import OpenAICompatClient


def load_system_rules() -> str:
    return (ROOT / "prompts" / "common" / "system_rules.md").read_text(encoding="utf-8")


def make_client() -> OpenAICompatClient:
    base_url = os.getenv("LLM_BASE_URL", "http://localhost:8000/v1")
    model = os.getenv("LLM_MODEL", "kakaocorp/kanana-1.5-8b-instruct-2505")
    api_key = os.getenv("LLM_API_KEY", "")
    return OpenAICompatClient(base_url=base_url, api_key=api_key, model=model)


def discover_section_dirs() -> list[Path]:
    sections_root = ROOT / "src" / "sections"
    return sorted([p.parent for p in sections_root.rglob("inputs_spec.json")])


def run_async(coro):
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)


st.set_page_config(page_title="DART 재무 리포트 생성 데모", layout="centered")
st.title("DART 재무 리포트 생성 데모")

corp_name = st.text_input("기업명 (corp_name)", value="")
bsns_year = st.number_input("연도 (bsns_year)", value=2024, step=1)

# 전 섹션 자동 실행
section_dirs = discover_section_dirs()

if st.button("리포트 생성"):
    corp_name = corp_name.strip()
    if not corp_name:
        st.error("기업명을 입력해 주세요.")
        st.stop()

    try:
        client = make_client()
        system_rules = load_system_rules()

        workdir_root = ROOT / "workdir"
        out_dir = ROOT / "outputs" / "sections" / corp_name / str(int(bsns_year))
        out_dir.mkdir(parents=True, exist_ok=True)

        with st.spinner("입력 생성 + 섹션 병렬 실행 중..."):
            run_async(
                run_sections_parallel(
                    workdir_root=workdir_root,
                    section_dirs=section_dirs,     
                    client=client,
                    system_rules=system_rules,
                    out_dir=out_dir,
                    company=corp_name,
                    year=int(bsns_year),
                    build_inputs=True,
                    build_inputs_sequential=True,
                )
            )

        st.success("완료!")
        st.write("결과 위치:", str(out_dir))

    except Exception as e:
        st.error(f"실행 중 오류: {e}")

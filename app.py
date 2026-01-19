# app.py
import sys
import os
import asyncio
from pathlib import Path
from scripts.build_report_pdf import build_pdf

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


def run_async(coro_factory):
    """
    coro_factory: 호출할 때마다 '새로운 coroutine'을 만들어 반환하는 함수
    예: lambda: run_sections_parallel(...)
    """
    try:
        return asyncio.run(coro_factory())
    except RuntimeError:
        # Streamlit thread에는 event loop가 없을 수 있으니 새로 만든다
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro_factory())
        finally:
            loop.close()
            asyncio.set_event_loop(None)



st.set_page_config(page_title="DART 재무 리포트 생성 데모", layout="centered")
st.title("DART 재무 리포트 생성 데모")

corp_name = st.text_input("기업명 (corp_name)", value="")
bsns_year = st.number_input("연도 (bsns_year)", value=2024, step=1)

# ✅ 전 섹션 자동 실행
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
            run_async(lambda: run_sections_parallel(
                workdir_root=workdir_root,
                section_dirs=[ROOT / "src" / "sections" / "c02_bs" / "s02_1_assets",],
                client=client,
                system_rules=system_rules,
                out_dir=out_dir,
                company=corp_name,
                year=int(bsns_year),
                build_inputs=True,
                build_inputs_sequential=True,
            ))
        st.success("완료!")
        st.write("결과 위치:", str(out_dir))

         # ✅ PDF 생성
        pdf_path = out_dir / f"{corp_name}_{int(bsns_year)}_report.pdf"
        font_path = Path("/usr/share/fonts/truetype/nanum/NanumGothic.ttf")

        build_pdf(
            sections_dir=out_dir,     
            out_pdf=pdf_path,
            font_path=font_path,
            report_title=f"{corp_name} {int(bsns_year)} 재무분석 리포트",
        )

        # ✅ 다운로드 버튼
        with open(pdf_path, "rb") as f:
            st.download_button(
                label="PDF 다운로드",
                data=f,
                file_name=pdf_path.name,
                mime="application/pdf",
            )

    except Exception as e:
        st.exception(e)

    

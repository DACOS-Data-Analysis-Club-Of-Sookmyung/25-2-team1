# app.py
import sys
import os
import json
from pathlib import Path

# --- path fix: repo 어디서 실행해도 src 모듈 import 되게 ---
ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SRC))
# -----------------------------------------------------------

import streamlit as st

from generate import run_sections_parallel
from llm.client import OpenAICompatClient


def load_system_rules() -> str:
    p = ROOT / "prompts" / "common" / "system_rules.md"
    return p.read_text(encoding="utf-8")


def make_client() -> OpenAICompatClient:
    base_url = os.getenv("LLM_BASE_URL", "http://localhost:8000/v1")
    model = os.getenv("LLM_MODEL", "kakaocorp/kanana-1.5-8b-instruct-2505")
    api_key = os.getenv("LLM_API_KEY", "")  # 로컬 vLLM이면 빈 문자열이어도 OK

    return OpenAICompatClient(
        base_url=base_url,
        api_key=api_key,
        model=model,
    )


st.set_page_config(page_title="DART 재무 리포트 생성 데모", layout="centered")
st.title("DART 재무 리포트 생성 데모")

# ✅ 입력은 corp_name, bsns_year만
corp_name = st.text_input("기업명 (corp_name)", value="")
bsns_year = st.number_input("연도 (bsns_year)", value=2024, step=1)

# (데모용) 현재 연결 설정 표시
with st.expander("현재 LLM 설정 보기", expanded=False):
    st.write("LLM_BASE_URL:", os.getenv("LLM_BASE_URL", "http://localhost:8000/v1"))
    st.write("LLM_MODEL:", os.getenv("LLM_MODEL", "kakaocorp/kanana-1.5-8b-instruct-2505"))
    st.write("LLM_API_KEY:", "(empty)" if not os.getenv("LLM_API_KEY") else "(set)")

if st.button("리포트 생성"):
    try:
        corp_name = corp_name.strip()
        if not corp_name:
            st.error("기업명을 입력해 주세요.")
            st.stop()

        workdir = ROOT / "workdir"
        (workdir / "meta").mkdir(parents=True, exist_ok=True)
        meta = {"corp_name": corp_name, "bsns_year": int(bsns_year)}
        (workdir / "meta" / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        system_rules = load_system_rules()
        client = make_client()

        out_dir = ROOT / "outputs" / "sections"
        out_dir.mkdir(parents=True, exist_ok=True)

        with st.spinner("생성 중... (섹션 병렬 실행)"):
            run_sections_parallel(workdir, client, system_rules, out_dir)

        st.success("완료! outputs/sections 를 확인하세요.")

    except Exception as e:
        st.error(f"실행 중 오류: {e}")

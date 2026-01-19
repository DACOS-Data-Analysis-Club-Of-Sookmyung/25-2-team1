# scripts/test_one_section.py

from pathlib import Path
import os

from src.generate import run_section
from src.llm.client import OpenAICompatClient


def main():
    workdir = Path("workdir")
    section_dir = Path("src/sections/1.2")  
    out_dir = Path("outputs/sections")
    out_dir.mkdir(parents=True, exist_ok=True)

    base_url = os.getenv("KANANA_BASE_URL", "http://localhost:8000/v1")
    api_key  = os.getenv("KANANA_API_KEY", "")  # 로컬 vLLM이면 빈 값이어도 되는 경우 많음
    model    = os.getenv("KANANA_MODEL", "kakao/kanana-1.5-8b")  # 예시: 네가 서빙한 모델 ID로

    client = OpenAICompatClient(base_url=base_url, api_key=api_key, model=model)

    system_rules = Path("src/system_rules.txt").read_text(encoding="utf-8")

    out_path = run_section(
        workdir=workdir,
        section_dir=section_dir,
        client=client,
        system_rules=system_rules,
        out_dir=out_dir,
    )

    print("saved:", out_path)
    print(out_path.read_text(encoding="utf-8")[:800])


if __name__ == "__main__":
    main()

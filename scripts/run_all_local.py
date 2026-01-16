import asyncio
import json
from pathlib import Path

from src.generate import run_section, run_sections_parallel, make_bridge_summary

SECTIONS_ROOT = Path("src/sections")
OUT_SECTIONS_DIR = Path("outputs/sections")
OUT_SECTIONS_DIR.mkdir(parents=True, exist_ok=True)

def spec_id_of(section_dir: Path) -> str:
    spec = json.loads((section_dir / "inputs_spec.json").read_text(encoding="utf-8"))
    return spec["id"]

def main():
    # workdir, client, system_rules 준비는 기존 run_all_local.py 그대로 사용

    # 1) Phase1 (1~8) - 병렬
    phase1_dirs = [
        # TODO: 너희 실제 섹션 폴더들로 채우기 (1~8 전체)
    ]
    phase1_spec_ids = [spec_id_of(d) for d in phase1_dirs]

    asyncio.run(run_sections_parallel(
        workdir=workdir,
        section_dirs=phase1_dirs,
        client=client,
        system_rules=system_rules,
        out_dir=OUT_SECTIONS_DIR,
    ))

    # 2) Bridge 저장: workdir/bridge_summary.json
    make_bridge_summary(
        client=client,
        out_dir=OUT_SECTIONS_DIR,
        workdir=workdir,
        phase1_spec_ids=phase1_spec_ids,
    )

    # 3) Phase2 (9장) - 순차
    phase2_dirs = [
        SECTIONS_ROOT / "c09_swot" / "s09_1_strength",
        SECTIONS_ROOT / "c09_swot" / "s09_2_weakness",
        SECTIONS_ROOT / "c09_swot" / "s09_3_opportunity",
        SECTIONS_ROOT / "c09_swot" / "s09_4_threat",
    ]
    for d in phase2_dirs:
        run_section(workdir, d, client, system_rules, OUT_SECTIONS_DIR)

    # 4) Phase3 (10장) - 순차
    phase3_dirs = [
        SECTIONS_ROOT / "c10_conclusion" / "s10_1_strength_summary",
        SECTIONS_ROOT / "c10_conclusion" / "s10_2_risk_summary",
        SECTIONS_ROOT / "c10_conclusion" / "s10_3_implications_limits",
    ]
    for d in phase3_dirs:
        run_section(workdir, d, client, system_rules, OUT_SECTIONS_DIR)


if __name__ == "__main__":
    main()

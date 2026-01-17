import os
import asyncio
import json
from pathlib import Path

from src.generate import run_section, run_sections_parallel, make_bridge_summary
from src.llm.client import OpenAICompatClient

SECTIONS_ROOT = Path("src/sections")
OUT_SECTIONS_DIR = Path("outputs/sections")
OUT_SECTIONS_DIR.mkdir(parents=True, exist_ok=True)


def spec_id_of(section_dir: Path) -> str:
    spec = json.loads((section_dir / "inputs_spec.json").read_text(encoding="utf-8"))
    return spec["id"]


def _find_project_root(start: Path) -> Path | None:
    cur = start.resolve()
    for _ in range(12):
        if (cur / "prompts" / "common" / "system_rules.md").exists():
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return None


def load_system_rules(workdir: Path) -> str:
    cand: list[Path] = []

    env_path = os.getenv("SYSTEM_RULES_PATH")
    if env_path:
        cand.append(Path(env_path))

    cand += [
        workdir / "prompts" / "common" / "system_rules.md",
        workdir / "system_rules.md",
    ]

    here = Path(__file__).resolve().parent
    root = _find_project_root(here)
    if root is not None:
        cand.append(root / "prompts" / "common" / "system_rules.md")

    cand += [
        Path("prompts/common/system_rules.md"),
        Path("system_rules.md"),
    ]

    for p in cand:
        try:
            if p.exists():
                return p.read_text(encoding="utf-8")
        except OSError:
            continue

    return (
        "너는 재무제표 기반 분석 보고서를 작성하는 조력자다. "
        "입력 데이터(표/근거)를 벗어난 숫자나 사실을 생성하지 마라. "
        "근거 표기가 필요한 경우 입력에 있는 식별자(note_no, section_code, chunk_id)를 사용해라."
    )


def build_client() -> OpenAICompatClient:
    base_url = os.getenv("LLM_BASE_URL", "http://localhost:8000/v1")
    api_key = os.getenv("LLM_API_KEY", "")
    model = os.getenv("LLM_MODEL", "kanana-1.5-8b-instruct")

    return OpenAICompatClient(
        base_url=base_url,
        api_key=api_key,
        model=model,
        timeout=int(os.getenv("LLM_TIMEOUT", "120")),
    )


def ensure_workdir_layout(workdir: Path) -> None:
    (workdir / "meta").mkdir(parents=True, exist_ok=True)
    (workdir / "metrics").mkdir(parents=True, exist_ok=True)
    (workdir / "evidence").mkdir(parents=True, exist_ok=True)
    (workdir / "summary").mkdir(parents=True, exist_ok=True)


def main():
    # 0) workdir / system_rules / client 준비
    workdir = Path(os.getenv("WORKDIR", "workdir")).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    ensure_workdir_layout(workdir)

    client = build_client()
    system_rules = load_system_rules(workdir)

    print(f"[INFO] workdir = {workdir}")
    print(f"[INFO] meta policy = {workdir / 'meta' / 'meta.json'}")
    print(f"[INFO] out_dir = {OUT_SECTIONS_DIR}")
    print(f"[INFO] system_rules loaded (len={len(system_rules)})")

    # Phase1 (1~8) - 병렬
    phase1_dirs = [
        SECTIONS_ROOT / "c01_intro" / "s01_1_objective",
        SECTIONS_ROOT / "c01_intro" / "s01_2_company_overview",
        SECTIONS_ROOT / "c01_intro" / "s01_3_data_scope",

        SECTIONS_ROOT / "c02_bs" / "s02_1_assets",
        SECTIONS_ROOT / "c02_bs" / "s02_2_liabilities",
        SECTIONS_ROOT / "c02_bs" / "s02_3_equity",
        SECTIONS_ROOT / "c02_bs" / "s02_4_financial_health",

        SECTIONS_ROOT / "c03_is" / "s03_1_rev_cost",
        SECTIONS_ROOT / "c03_is" / "s03_2_profit_flow",
        SECTIONS_ROOT / "c03_is" / "s03_3_profitability_summary",

        SECTIONS_ROOT / "c04_cf" / "s04_1_ocf",
        SECTIONS_ROOT / "c04_cf" / "s04_2_icf",
        SECTIONS_ROOT / "c04_cf" / "s04_3_fff",

        SECTIONS_ROOT / "c05_liquidity" / "s05_1_current_ratio",
        SECTIONS_ROOT / "c05_liquidity" / "s05_2_quick_ratio",
        SECTIONS_ROOT / "c05_liquidity" / "s05_3_cash_ratio",
        SECTIONS_ROOT / "c05_liquidity" / "s05_4_liquidity_summary",

        SECTIONS_ROOT / "c06_leverage_stability" / "s06_1_total_debt_ratio",
        SECTIONS_ROOT / "c06_leverage_stability" / "s06_2_long_term_debt_ratio",
        SECTIONS_ROOT / "c06_leverage_stability" / "s06_3_interest_coverage",
        SECTIONS_ROOT / "c06_leverage_stability" / "s06_4_cash_coverage_ocf",
        SECTIONS_ROOT / "c06_leverage_stability" / "s06_5_stability_summary",

        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_1_turnover",
        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_2_roa_roe_roc",
        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_3_dupont",
        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_4_market_ratios",
        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_5_ch7_summary",

        SECTIONS_ROOT / "c08_activity_pros_cons" / "s08_1_operating",
        SECTIONS_ROOT / "c08_activity_pros_cons" / "s08_2_investing",
        SECTIONS_ROOT / "c08_activity_pros_cons" / "s08_3_financing",
    ]
    phase1_spec_ids = [spec_id_of(d) for d in phase1_dirs]

    print(f"[PHASE1] run parallel sections: {len(phase1_dirs)}")
    asyncio.run(
        run_sections_parallel(
            workdir=workdir,
            section_dirs=phase1_dirs,
            client=client,
            system_rules=system_rules,
            out_dir=OUT_SECTIONS_DIR,
            build_inputs=True,
            build_inputs_sequential=True,  
        )
    )

    # Bridge (1~8)
    print("[BRIDGE] make summary for chapters 1~8")
    make_bridge_summary(
        client=client,
        out_dir=OUT_SECTIONS_DIR,
        workdir=workdir,
        phase1_spec_ids=phase1_spec_ids,
        chapter_groups=[(1, 2), (3, 4, 5), (6, 7, 8)],
    )

    # Phase2 (9장) - 순차
    phase2_dirs = [
        SECTIONS_ROOT / "c09_swot" / "s09_1_strength",
        SECTIONS_ROOT / "c09_swot" / "s09_2_weakness",
        SECTIONS_ROOT / "c09_swot" / "s09_3_opportunity",
        SECTIONS_ROOT / "c09_swot" / "s09_4_threat",
    ]
    phase2_spec_ids = [spec_id_of(d) for d in phase2_dirs]

    print(f"[PHASE2] run sequential sections: {len(phase2_dirs)} (chapter 9)")
    for d in phase2_dirs:
        run_section(workdir, d, client, system_rules, OUT_SECTIONS_DIR, build_inputs=True)

    # Bridge (9)
    print("[BRIDGE] make summary for chapter 9")
    make_bridge_summary(
        client=client,
        out_dir=OUT_SECTIONS_DIR,
        workdir=workdir,
        phase1_spec_ids=phase2_spec_ids,
        chapter_groups=[(9,)],
    )

    # Phase3 (10장) - 순차
    phase3_dirs = [
        SECTIONS_ROOT / "c10_conclusion" / "s10_1_strength_summary",
        SECTIONS_ROOT / "c10_conclusion" / "s10_2_risk_summary",
        SECTIONS_ROOT / "c10_conclusion" / "s10_3_implication_and_limits",
    ]

    print(f"[PHASE3] run sequential sections: {len(phase3_dirs)} (chapter 10)")
    for d in phase3_dirs:
        run_section(workdir, d, client, system_rules, OUT_SECTIONS_DIR, build_inputs=True)

    print("[DONE] all sections generated.")


if __name__ == "__main__":
    main()

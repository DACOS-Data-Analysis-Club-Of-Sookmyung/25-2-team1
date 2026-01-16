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

    # 1) Phase1 (1~8) - 병렬
        # 1) Phase1 (1~8) - 병렬
    phase1_dirs = [
        # --- c01_intro ---
        SECTIONS_ROOT / "c01_intro" / "s01_1_objective",
        SECTIONS_ROOT / "c01_intro" / "s01_2_company_overview",
        SECTIONS_ROOT / "c01_intro" / "s01_3_data_scope",

        # --- c02_bs ---
        SECTIONS_ROOT / "c02_bs" / "s02_1_assets",
        SECTIONS_ROOT / "c02_bs" / "s02_2_liabilities",
        SECTIONS_ROOT / "c02_bs" / "s02_3_equity",
        SECTIONS_ROOT / "c02_bs" / "s02_4_financial_health",

        # --- c03_is ---
        SECTIONS_ROOT / "c03_is" / "s03_1_rev_cost",
        SECTIONS_ROOT / "c03_is" / "s03_2_profit_flow",
        SECTIONS_ROOT / "c03_is" / "s03_3_profitability_summary",

        # --- c04_cf ---
        SECTIONS_ROOT / "c04_cf" / "s04_1_ocf",
        SECTIONS_ROOT / "c04_cf" / "s04_2_icf",
        SECTIONS_ROOT / "c04_cf" / "s04_3_fff",

        # --- c05_liquidity ---
        SECTIONS_ROOT / "c05_liquidity" / "s05_1_current_ratio",
        SECTIONS_ROOT / "c05_liquidity" / "s05_2_quick_ratio",
        SECTIONS_ROOT / "c05_liquidity" / "s05_3_cash_ratio",
        SECTIONS_ROOT / "c05_liquidity" / "s05_4_liquidity_summary",

        # --- c06_leverage_stability ---
        SECTIONS_ROOT / "c06_leverage_stability" / "s06_1_total_debt_ratio",
        SECTIONS_ROOT / "c06_leverage_stability" / "s06_2_long_term_debt_ratio",
        SECTIONS_ROOT / "c06_leverage_stability" / "s06_3_interest_coverage",
        SECTIONS_ROOT / "c06_leverage_stability" / "s06_4_cash_coverage_ocf",
        SECTIONS_ROOT / "c06_leverage_stability" / "s06_5_stability_summary",

        # --- c07_profitability_dupont ---
        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_1_turnover",
        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_2_roa_roe_roc",
        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_3_dupont",
        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_4_market_ratios",
        SECTIONS_ROOT / "c07_profitability_dupont" / "s07_5_ch7_summary",

        # --- c08_activity_pros_cons ---
        SECTIONS_ROOT / "c08_activity_pros_cons" / "s08_1_operating",
        SECTIONS_ROOT / "c08_activity_pros_cons" / "s08_2_investing",
        SECTIONS_ROOT / "c08_activity_pros_cons" / "s08_3_financing",
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
        SECTIONS_ROOT / "c10_conclusion" / "s10_3_implication_and_limits",
    ]
    for d in phase3_dirs:
        run_section(workdir, d, client, system_rules, OUT_SECTIONS_DIR)


if __name__ == "__main__":
    main()

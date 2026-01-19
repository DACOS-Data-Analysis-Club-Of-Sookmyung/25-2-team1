# run_calc.py
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import argparse
import json
from typing import List

import duckdb
import pandas as pd

from src.calc import (
    load_fact_metrics,
    update_benchmark_improved,
)
from src.validate import (
    fetch_fact_metrics,
    fetch_metric_catalog,
    fetch_ratio_requirements,
    fetch_value_augmented,
    validate_coverage,
    validate_catalog_alignment,
    validate_raw_rows,
    validate_ratio_rows,
)


# ============================================================
# JSON 출력 유틸
# ============================================================

def export_metrics_json(
    con,
    corp_code: str,
    bsns_year: int,
    metrics_spec: List[str],
    out_path: str,
):
    df = fetch_fact_metrics(con, corp_code, bsns_year, metrics_spec)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            "metric_key": r["metric_key"],
            "metric_name_ko": r["metric_name_ko"],
            "metric_type": r["metric_type"],
            "value": None if pd.isna(r["value"]) else float(r["value"]),
            "value_prev": None if pd.isna(r["value_prev"]) else float(r["value_prev"]),
            "yoy_abs": None if pd.isna(r["yoy_abs"]) else float(r["yoy_abs"]),
            "yoy_pct": None if pd.isna(r["yoy_pct"]) else float(r["yoy_pct"]),
            "unit": r["unit"],
            "benchmark_corp_code": (
                None if pd.isna(r["benchmark_corp_code"]) else r["benchmark_corp_code"]
            ),
            "benchmark_value": (
                None if pd.isna(r["benchmark_value"]) else float(r["benchmark_value"])
            ),
            "benchmark_improved": (
                None if pd.isna(r["benchmark_improved"]) else bool(r["benchmark_improved"])
            ),
        })

    payload = {
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "rows": rows,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"✅ metrics.json written → {out_path}")


# ============================================================
# 검증 실행
# ============================================================

def run_validation(
    con,
    corp_code: str,
    bsns_year: int,
    metrics_spec: List[str],
):
    df = fetch_fact_metrics(con, corp_code, bsns_year, metrics_spec)
    catalog = fetch_metric_catalog(con)
    ratio_req = fetch_ratio_requirements(con)
    value_aug = fetch_value_augmented(con, corp_code, bsns_year)

    checks = []
    checks += validate_coverage(df, metrics_spec)
    checks += validate_catalog_alignment(df, catalog)
    checks += validate_raw_rows(df)
    checks += validate_ratio_rows(df, ratio_req, value_aug)

    summary = {"PASS": 0, "WARN": 0, "FAIL": 0}
    for c in checks:
        summary[c.level] += 1

    print("\n=== VALIDATION SUMMARY ===")
    print(summary)

    fails = [c for c in checks if c.level == "FAIL"]
    if fails:
        print("\n=== FAIL DETAILS (top 20) ===")
        for c in fails[:20]:
            print(f"- {c.metric_key}: {c.message}")
        raise SystemExit("❌ Validation failed")

    print("✅ All validations passed")
    return summary, checks


# ============================================================
# main
# ============================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="duckdb file path")
    ap.add_argument("--corp_code", required=True)
    ap.add_argument("--bsns_year", type=int, required=True)
    ap.add_argument(
        "--metrics_spec",
        nargs="+",
        required=True,
        help="metric keys list",
    )
    ap.add_argument("--out", default="metrics.json")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=False)

    print("🚀 STEP 1: fact_metrics 적재")
    load_fact_metrics(
        con,
        corp_code=args.corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=args.metrics_spec,
    )

    print("🚀 STEP 2: benchmark_improved 계산")
    update_benchmark_improved(
        con,
        corp_code=args.corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=args.metrics_spec,
    )

    print("🚀 STEP 3: 검증 파이프라인 실행")
    run_validation(
        con,
        corp_code=args.corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=args.metrics_spec,
    )

    print("🚀 STEP 4: JSON 출력")
    export_metrics_json(
        con,
        corp_code=args.corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=args.metrics_spec,
        out_path=args.out,
    )

    print("🎉 run_calc.py completed successfully")


if __name__ == "__main__":
    main()

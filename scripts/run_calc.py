# scripts/run_calc.py

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
    build_account_map_rules,
    create_calc_views,
    create_metric_catalog,
    load_fact_metrics,
    update_benchmark_values,
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
# request_context / request_metrics 주입 유틸
# ============================================================

def _normalize_metrics_spec(metrics_spec: List[str]) -> List[str]:
    """공백 제거 + 중복 제거 + 빈값 제거"""
    uniq: List[str] = []
    seen = set()
    for m in metrics_spec or []:
        m = str(m).strip()
        if not m or m in seen:
            continue
        seen.add(m)
        uniq.append(m)
    return uniq


def inject_request_context(
    con,
    corp_code: str,
    bsns_year: int,
    store_prev_year: bool = True,
):
    """
    계산/검증에서 공통으로 참조 가능한 '요청 컨텍스트'를 TEMP로 주입
    ✅ report_id는 넣지 않음 (report_id 중복은 view/적재 단계에서 dedup 처리)
    """
    prev_year = int(bsns_year) - 1 if store_prev_year else None

    con.execute("DROP TABLE IF EXISTS request_context;")
    con.execute("""
    CREATE TEMP TABLE request_context (
      corp_code VARCHAR,
      bsns_year INTEGER,
      prev_year INTEGER
    );
    """)

    con.execute(
        "INSERT INTO request_context VALUES (?, ?, ?)",
        [corp_code, int(bsns_year), prev_year],
    )

    print("✅ request_context injected:", {"corp_code": corp_code, "bsns_year": bsns_year, "prev_year": prev_year})


def inject_request_metrics(con, metrics_spec: List[str]) -> List[str]:
    """
    inputs_spec.json/request에서 내려온 metrics_spec을 TEMP로 주입
    """
    metrics_spec = _normalize_metrics_spec(metrics_spec)
    if not metrics_spec:
        raise ValueError("metrics_spec is empty")

    con.execute("DROP TABLE IF EXISTS request_metrics;")
    con.execute("""
    CREATE TEMP TABLE request_metrics (
      metric_key VARCHAR PRIMARY KEY
    );
    """)

    con.executemany(
        "INSERT OR IGNORE INTO request_metrics VALUES (?)",
        [(k,) for k in metrics_spec],
    )

    print("✅ request_metrics injected:", metrics_spec)
    return metrics_spec


# ============================================================
# ✅ 초기화 보장 (뷰/테이블/카탈로그 생성)
# ============================================================

def assert_required_tables(con):
    # ✅ benchmark 계산에 benchmark_map이 필수
    required = ["fs_facts", "fs_line_items", "reports", "rag_table_rows", "market_data", "benchmark_map"]
    missing = []
    for t in required:
        ok = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name=?",
            [t],
        ).fetchone()
        if not ok:
            missing.append(t)
    if missing:
        raise RuntimeError(
            f"필수 테이블이 없습니다: {missing}. run_ingest.py 및 market/benchmark 적재 후 다시 실행하세요."
        )


def ensure_calc_initialized(con):
    """
    calc.py에서 계산에 필요한 매핑룰/뷰/카탈로그를 항상 준비.
    - idempotent: 매번 실행해도 동일 상태로 재구성됨
    """
    print("🧱 INIT: build account_map_rules")
    build_account_map_rules(con)

    print("🧱 INIT: create calc views (v_analysis_compare, v_value_augmented, v_financial_ratios, ratio_requirements...)")
    create_calc_views(con)

    print("🧱 INIT: create metric_catalog")
    create_metric_catalog(con)

    print("✅ INIT completed")


# ============================================================
# JSON 출력 유틸
# ============================================================

def export_metrics_json(con, corp_code: str, bsns_year: int, metrics_spec: List[str], out_path: str):
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
            "benchmark_corp_code": None if pd.isna(r["benchmark_corp_code"]) else r["benchmark_corp_code"],
            "benchmark_value": None if pd.isna(r["benchmark_value"]) else float(r["benchmark_value"]),
            "benchmark_improved": None if pd.isna(r["benchmark_improved"]) else bool(r["benchmark_improved"]),
        })

    payload = {"corp_code": corp_code, "bsns_year": bsns_year, "rows": rows}

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"✅ metrics.json written → {out_path}")


# ============================================================
# 검증 실행
# ============================================================

def run_validation(con, corp_code: str, bsns_year: int, metrics_spec: List[str]):
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
    ap.add_argument("--metrics_spec", nargs="+", required=True, help="metric keys list")
    ap.add_argument("--out", default="metrics.json")
    ap.add_argument("--no_init", action="store_true", help="skip init (assumes views/catalog already exist)")

    args = ap.parse_args()
    con = duckdb.connect(args.db, read_only=False)

    # ✅ 0) 초기화 보장
    if not args.no_init:
        assert_required_tables(con)
        ensure_calc_initialized(con)

    # ✅ 주입(요청 컨텍스트/메트릭 목록)
    inject_request_context(con, args.corp_code, args.bsns_year, store_prev_year=True)
    metrics_spec = inject_request_metrics(con, args.metrics_spec)

    print("🚀 STEP 1: fact_metrics 적재 (target)")
    load_fact_metrics(
        con,
        corp_code=args.corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=metrics_spec,
    )

    print("🚀 STEP 1.5: benchmark corp_code 조회")
    bench_row = con.execute(
        """
        SELECT bench_corp_code
        FROM benchmark_map
        WHERE corp_code = ?
          AND year = ?
        """,
        [args.corp_code, args.bsns_year],
    ).fetchone()

    if not bench_row or not bench_row[0]:
        raise SystemExit(f"❌ benchmark_map missing: corp={args.corp_code}, year={args.bsns_year}")

    bench_corp_code = bench_row[0]
    print("✅ benchmark selected:", bench_corp_code)

    print("🚀 STEP 1.6: fact_metrics 적재 (benchmark)")
    load_fact_metrics(
        con,
        corp_code=bench_corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=metrics_spec,
    )

    print("🚀 STEP 2: benchmark_value 채우기")
    update_benchmark_values(
        con,
        corp_code=args.corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=metrics_spec,
    )

    print("🚀 STEP 3: benchmark_improved 계산")
    update_benchmark_improved(
        con,
        corp_code=args.corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=metrics_spec,
    )

    print("🚀 STEP 4: 검증 파이프라인 실행")
    run_validation(
        con,
        corp_code=args.corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=metrics_spec,
    )

    print("🚀 STEP 5: JSON 출력")
    export_metrics_json(
        con,
        corp_code=args.corp_code,
        bsns_year=args.bsns_year,
        metrics_spec=metrics_spec,
        out_path=args.out,
    )

    print("🎉 run_calc.py completed successfully")


if __name__ == "__main__":
    main()

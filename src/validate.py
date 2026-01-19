# src/validate.py

from __future__ import annotations

import duckdb
import pandas as pd


def validate_market_tables(con: duckdb.DuckDBPyConnection) -> dict:
    out: dict = {}

    out["market_data_rows"] = con.execute("SELECT COUNT(*) FROM market_data").fetchone()[0]
    out["benchmark_map_rows"] = con.execute("SELECT COUNT(*) FROM benchmark_map").fetchone()[0]

    # corp_code+year+corp_role 로 중복 체크
    dup_market = con.execute("""
    SELECT corp_code, year, corp_role, COUNT(*) AS cnt
    FROM market_data
    GROUP BY corp_code, year, corp_role
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    """).df()
    out["dup_market"] = dup_market

    dup_map = con.execute("""
    SELECT corp_code, year, COUNT(*) AS cnt
    FROM benchmark_map
    GROUP BY corp_code, year
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    """).df()
    out["dup_map"] = dup_map

    # benchmark_map의 bench_corp_code는 market_data에 'benchmark' role로 존재하는지 확인하는 게 정확함
    missing_bench = con.execute("""
    SELECT bm.corp_code, bm.year, bm.bench_corp_code
    FROM benchmark_map bm
    LEFT JOIN market_data m
      ON m.corp_code = bm.bench_corp_code
     AND m.year = bm.year
     AND m.corp_role = 'benchmark'
    WHERE m.corp_code IS NULL
    """).df()
    out["missing_bench_in_market_data"] = missing_bench

    required_cols = [
        "corp_code", "year", "stock_code", "asof_date",
        "stock_price", "shares_outstanding", "corp_role"
    ]
    null_stats = con.execute(f"""
    SELECT
      {", ".join([f"AVG(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS null_{c}" for c in required_cols])}
    FROM market_data
    """).df()
    out["null_stats"] = null_stats

    invalid_vals = con.execute("""
    SELECT corp_code, year, corp_role, stock_price, shares_outstanding, asof_date
    FROM market_data
    WHERE stock_price IS NULL
       OR shares_outstanding IS NULL
       OR stock_price <= 0
       OR shares_outstanding <= 0
    LIMIT 50
    """).df()
    out["invalid_price_shares"] = invalid_vals

    bad_date = con.execute("""
    SELECT corp_code, year, corp_role, asof_date
    FROM market_data
    WHERE try_cast(asof_date AS DATE) IS NULL
    LIMIT 50
    """).df()
    out["bad_asof_date"] = bad_date

    year_mismatch = con.execute("""
    SELECT corp_code, year, corp_role, asof_date, try_cast(asof_date AS DATE) AS asof_dt
    FROM market_data
    WHERE try_cast(asof_date AS DATE) IS NOT NULL
      AND EXTRACT(YEAR FROM try_cast(asof_date AS DATE)) <> year
    LIMIT 50
    """).df()
    out["asof_year_mismatch"] = year_mismatch

    self_bench = con.execute("""
    SELECT corp_code, year, bench_corp_code, benchmark_name_kr
    FROM benchmark_map
    WHERE corp_code = bench_corp_code
    LIMIT 50
    """).df()
    out["self_benchmark"] = self_bench

    bench_cnt = con.execute("""
    SELECT corp_code, year, COUNT(DISTINCT bench_corp_code) AS bench_n
    FROM benchmark_map
    GROUP BY corp_code, year
    ORDER BY bench_n DESC
    LIMIT 20
    """).df()
    out["bench_cnt_top"] = bench_cnt

    multi_bench = con.execute("""
    SELECT *
    FROM (
      SELECT corp_code, year, COUNT(DISTINCT bench_corp_code) AS bench_n
      FROM benchmark_map
      GROUP BY corp_code, year
    )
    WHERE bench_n > 1
    LIMIT 50
    """).df()
    out["multi_benchmark"] = multi_bench

    return out


def validate_ingest_report(con: duckdb.DuckDBPyConnection, report_id: str) -> dict:
    """
    ingest 결과 QC: section/table/chunk/fact/link 개수 체크.
    """
    out: dict = {}

    out["sections"] = con.execute("""
      SELECT section_type, COUNT(*) AS cnt
      FROM report_sections
      WHERE report_id=?
      GROUP BY section_type
      ORDER BY section_type
    """, [report_id]).df()

    out["tables"] = con.execute("""
      SELECT rt.statement_type, COUNT(*) AS cnt
      FROM rag_tables rt
      JOIN report_sections rs ON rs.section_id=rt.section_id
      WHERE rs.report_id=?
      GROUP BY rt.statement_type
      ORDER BY rt.statement_type
    """, [report_id]).df()

    out["chunks"] = con.execute("""
      SELECT section_type, COUNT(*) AS cnt
      FROM rag_text_chunks
      WHERE report_id=?
      GROUP BY section_type
      ORDER BY section_type
    """, [report_id]).df()

    out["fs_facts_cnt"] = con.execute(
        "SELECT COUNT(*) FROM fs_facts WHERE report_id=?",
        [report_id]
    ).fetchone()[0]

    out["note_links_cnt"] = con.execute(
        "SELECT COUNT(*) FROM note_links WHERE report_id=?",
        [report_id]
    ).fetchone()[0]

    return out


#------------------------------
# 계산 검증 파이프라인
#------------------------------

import argparse
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd


EPS = 1e-9  # float 비교 허용 오차


def is_close(a: Optional[float], b: Optional[float], eps: float = EPS) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    # 상대/절대 혼합
    diff = abs(a - b)
    return diff <= eps or diff <= eps * max(1.0, abs(a), abs(b))


def to_py_float(x) -> Optional[float]:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    return float(x)


@dataclass
class CheckResult:
    level: str   # PASS / WARN / FAIL
    metric_key: Optional[str]
    message: str


def fetch_fact_metrics(con, corp_code: str, bsns_year: int, metrics_spec: List[str]) -> pd.DataFrame:
    # metrics_spec만 필터링해서 검증 (불필요한 연도 데이터 나오지 않게)
    placeholders = ",".join(["?"] * len(metrics_spec))
    q = f"""
    SELECT
      corp_code, bsns_year, metric_key, metric_name_ko, metric_type,
      value, value_prev, yoy_abs, yoy_pct, unit,
      benchmark_corp_code, benchmark_value, benchmark_improved
    FROM fact_metrics
    WHERE corp_code = ?
      AND bsns_year = ?
      AND metric_key IN ({placeholders})
    ORDER BY metric_type, metric_key;
    """
    return con.execute(q, [corp_code, bsns_year, *metrics_spec]).df()


def fetch_metric_catalog(con) -> pd.DataFrame:
    return con.execute("""
      SELECT metric_key, metric_type, unit, polarity
      FROM metric_catalog
    """).df()


def fetch_ratio_requirements(con) -> pd.DataFrame:
    return con.execute("""
      SELECT ratio_key, item_key, role, required
      FROM ratio_requirements
    """).df()


def fetch_value_augmented(con, corp_code: str, bsns_year: int) -> pd.DataFrame:
    # ratio 검증용 원천값 로드
    return con.execute("""
      SELECT std_key, value_won
      FROM v_value_augmented
      WHERE corp_code = ?
        AND bsns_year = ?
    """, [corp_code, bsns_year]).df()


def recompute_yoy(value: Optional[float], value_prev: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if value is None:
        return None, None
    if value_prev is None:
        return None, None
    yoy_abs = value - value_prev
    if value_prev == 0:
        yoy_pct = None
    else:
        yoy_pct = (value - value_prev) / abs(value_prev)
    return yoy_abs, yoy_pct


def validate_raw_rows(df: pd.DataFrame) -> List[CheckResult]:
    results: List[CheckResult] = []
    raw_df = df[df["metric_type"] == "raw"].copy()

    for _, r in raw_df.iterrows():
        key = r["metric_key"]
        value = to_py_float(r["value"])
        prev = to_py_float(r["value_prev"])
        yoy_abs = to_py_float(r["yoy_abs"])
        yoy_pct = to_py_float(r["yoy_pct"])

        exp_abs, exp_pct = recompute_yoy(value, prev)

        if not is_close(yoy_abs, exp_abs):
            results.append(CheckResult("FAIL", key, f"[raw] yoy_abs mismatch: db={yoy_abs} expected={exp_abs}"))
        else:
            results.append(CheckResult("PASS", key, f"[raw] yoy_abs OK"))

        if not is_close(yoy_pct, exp_pct):
            results.append(CheckResult("FAIL", key, f"[raw] yoy_pct mismatch: db={yoy_pct} expected={exp_pct}"))
        else:
            results.append(CheckResult("PASS", key, f"[raw] yoy_pct OK"))

        # 상식검증(옵션)
        if key in ("TOTAL_ASSETS", "CURRENT_ASSETS", "NON_CURRENT_ASSETS", "CASH_EQ") and value is not None and value < 0:
            results.append(CheckResult("WARN", key, f"[raw] value is negative for asset-like metric: {value}"))

    return results


def validate_ratio_rows(
    df_fact: pd.DataFrame,
    ratio_req: pd.DataFrame,
    value_aug: pd.DataFrame,
) -> List[CheckResult]:
    results: List[CheckResult] = []
    ratio_df = df_fact[df_fact["metric_type"] == "ratio"].copy()
    if ratio_df.empty:
        return results

    # std_key -> value_won map
    val_map = {row["std_key"]: to_py_float(row["value_won"]) for _, row in value_aug.iterrows()}

    # ratio_requirements를 ratio_key별로 묶어서 numerator/denominator 재구성
    grouped = ratio_req.groupby("ratio_key")

    for _, r in ratio_df.iterrows():
        ratio_key = r["metric_key"]
        db_ratio = to_py_float(r["value"])

        if ratio_key not in grouped.groups:
            results.append(CheckResult("FAIL", ratio_key, f"[ratio] ratio_requirements missing for {ratio_key}"))
            continue

        req_rows = ratio_req[ratio_req["ratio_key"] == ratio_key]

        # required 체크
        required_items = req_rows[req_rows["required"] == True]
        missing_required = []
        for _, rr in required_items.iterrows():
            item = rr["item_key"]
            if val_map.get(item) is None:
                missing_required.append(item)

        if missing_required:
            # DB ratio가 NULL이면 정상(계산불가), NULL이 아니면 문제
            if db_ratio is not None:
                results.append(CheckResult("FAIL", ratio_key, f"[ratio] should be NULL but has value. missing={missing_required}"))
            else:
                results.append(CheckResult("PASS", ratio_key, f"[ratio] correctly NULL (missing required={missing_required})"))
            continue

        # numerator/denominator 구성
        num = 0.0
        den = 0.0

        for _, rr in req_rows.iterrows():
            item = rr["item_key"]
            role = rr["role"]
            v = val_map.get(item)

            if v is None:
                continue

            if role == "numerator":
                num += v
            elif role == "denominator":
                den += v
            elif role == "add":
                num += v
            elif role == "subtract":
                num -= v

        exp_ratio = None
        if den is not None and den != 0:
            exp_ratio = num / den

        if not is_close(db_ratio, exp_ratio):
            results.append(CheckResult("FAIL", ratio_key, f"[ratio] ratio mismatch: db={db_ratio} expected={exp_ratio} (num={num}, den={den})"))
        else:
            results.append(CheckResult("PASS", ratio_key, f"[ratio] ratio OK"))

        # 범위 경고(옵션)
        if exp_ratio is not None and ratio_key in ("roe", "roa", "net_margin") and abs(exp_ratio) > 5:
            results.append(CheckResult("WARN", ratio_key, f"[ratio] unusually large ratio (>|5|): {exp_ratio}"))

    return results


def validate_coverage(df: pd.DataFrame, metrics_spec: List[str]) -> List[CheckResult]:
    results: List[CheckResult] = []
    got = set(df["metric_key"].tolist())
    want = set(metrics_spec)

    missing = sorted(list(want - got))
    extra = sorted(list(got - want))

    if missing:
        results.append(CheckResult("FAIL", None, f"[coverage] missing metrics in fact_metrics: {missing}"))
    else:
        results.append(CheckResult("PASS", None, "[coverage] all requested metrics present"))

    if extra:
        results.append(CheckResult("WARN", None, f"[coverage] extra metrics returned (not requested): {extra}"))

    return results


def validate_catalog_alignment(df: pd.DataFrame, catalog: pd.DataFrame) -> List[CheckResult]:
    results: List[CheckResult] = []
    cat = {row["metric_key"]: (row["metric_type"], row["unit"]) for _, row in catalog.iterrows()}

    for _, r in df.iterrows():
        key = r["metric_key"]
        mtype = r["metric_type"]
        unit = r["unit"]

        if key not in cat:
            results.append(CheckResult("FAIL", key, f"[catalog] metric_key not in metric_catalog: {key}"))
            continue

        exp_type, exp_unit = cat[key]

        # metric_type 불일치
        if exp_type != mtype:
            results.append(CheckResult("FAIL", key, f"[catalog] metric_type mismatch: db={mtype} catalog={exp_type}"))
        else:
            results.append(CheckResult("PASS", key, f"[catalog] metric_type OK"))

        # unit 불일치
        if exp_unit != unit:
            results.append(CheckResult("FAIL", key, f"[catalog] unit mismatch: db={unit} catalog={exp_unit}"))
        else:
            results.append(CheckResult("PASS", key, f"[catalog] unit OK"))

    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="duckdb file path (e.g., pipeline.duckdb)")
    ap.add_argument("--corp_code", required=True)
    ap.add_argument("--bsns_year", type=int, required=True)
    ap.add_argument("--metrics_spec", nargs="+", required=True, help="metric keys list")
    ap.add_argument("--out", default="validation_report.json")
    args = ap.parse_args()

    con = duckdb.connect(args.db, read_only=False)

    df = fetch_fact_metrics(con, args.corp_code, args.bsns_year, args.metrics_spec)
    catalog = fetch_metric_catalog(con)
    ratio_req = fetch_ratio_requirements(con)
    value_aug = fetch_value_augmented(con, args.corp_code, args.bsns_year)

    checks: List[CheckResult] = []
    checks += validate_coverage(df, args.metrics_spec)
    checks += validate_catalog_alignment(df, catalog)
    checks += validate_raw_rows(df)
    checks += validate_ratio_rows(df, ratio_req, value_aug)

    # 요약
    summary = {"PASS": 0, "WARN": 0, "FAIL": 0}
    for c in checks:
        summary[c.level] += 1

    report = {
        "corp_code": args.corp_code,
        "bsns_year": args.bsns_year,
        "metrics_spec": args.metrics_spec,
        "summary": summary,
        "checks": [
            {"level": c.level, "metric_key": c.metric_key, "message": c.message}
            for c in checks
        ],
    }

    import json
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # 콘솔 출력
    print("=== VALIDATION SUMMARY ===")
    print(summary)
    fails = [c for c in checks if c.level == "FAIL"]
    if fails:
        print("\n=== FAIL DETAILS (top 20) ===")
        for c in fails[:20]:
            print(f"- {c.metric_key}: {c.message}")
        raise SystemExit(1)
    else:
        print("\n✅ All critical checks passed.")


if __name__ == "__main__":
    main()

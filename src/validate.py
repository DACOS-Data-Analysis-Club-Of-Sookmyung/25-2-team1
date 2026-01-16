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

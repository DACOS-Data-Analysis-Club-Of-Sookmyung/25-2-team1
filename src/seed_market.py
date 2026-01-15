# src/seed_market.py
from __future__ import annotations

import pandas as pd
import duckdb


def seed_market_from_csv(db_path: str, csv_path: str, overwrite: bool = True) -> None:
    """
    company_meta.csv(또는 with_benchmark.csv) -> market_data, benchmark_map 적재

    기대 컬럼(최소):
      corp_code, corp_name_kr, corp_name_en, stock_code, year, rcept_date, stock_price, shares_outstanding, scale,
      bench_corp_code, benchmark_name_kr, bench_stock_code, bench_rcept_date, bench_stock_price, bench_shares_outstanding
    """
    con = duckdb.connect(db_path)

    df = pd.read_csv(csv_path)

    # --- 타입/키 정리 (노트북 로직 그대로) ---
    df["corp_code"] = df["corp_code"].astype(str).str.zfill(8)
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    # bench_corp_code 안전 처리
    df["bench_corp_code"] = df["bench_corp_code"].astype(str).str.replace(".0", "", regex=False)
    df.loc[df["bench_corp_code"].isin(["nan", "None", "<NA>"]), "bench_corp_code"] = pd.NA
    df["bench_corp_code"] = df["bench_corp_code"].astype("string")

    mask = df["bench_corp_code"].notna()
    df.loc[mask, "bench_corp_code"] = df.loc[mask, "bench_corp_code"].str.zfill(8)

    # --- market_data: target ---
    market_df = df[
        [
            "corp_code", "corp_name_kr", "corp_name_en",
            "stock_code", "year", "rcept_date",
            "stock_price", "shares_outstanding", "scale"
        ]
    ].copy()
    market_df = market_df.rename(columns={"rcept_date": "asof_date"})
    market_df["price_basis"] = "rcept_date"
    market_df["corp_role"] = "target"

    # --- market_data: benchmark도 market_data에 저장 ---
    bench_market_df = df[
        [
            "bench_corp_code", "benchmark_name_kr",
            "bench_stock_code", "year", "bench_rcept_date",
            "bench_stock_price", "bench_shares_outstanding"
        ]
    ].copy()
    bench_market_df = bench_market_df.rename(columns={
        "bench_corp_code": "corp_code",
        "benchmark_name_kr": "corp_name_kr",
        "bench_stock_code": "stock_code",
        "bench_rcept_date": "asof_date",
        "bench_stock_price": "stock_price",
        "bench_shares_outstanding": "shares_outstanding",
    })
    bench_market_df["corp_name_en"] = pd.NA
    bench_market_df["scale"] = pd.NA
    bench_market_df["price_basis"] = "rcept_date"
    bench_market_df["corp_role"] = "benchmark"

    all_market = pd.concat([market_df, bench_market_df], ignore_index=True)

    # --- benchmark_map ---
    bench_map_df = df[["corp_code", "year", "benchmark_name_kr", "bench_corp_code"]].copy()

    # --- 테이블 생성/적재 ---
    if overwrite:
        con.execute("DROP TABLE IF EXISTS market_data")
        con.execute("DROP TABLE IF EXISTS benchmark_map")

    con.execute("""
      CREATE TABLE IF NOT EXISTS market_data (
        corp_code VARCHAR,
        corp_name_kr VARCHAR,
        corp_name_en VARCHAR,
        stock_code VARCHAR,
        year INTEGER,
        asof_date BIGINT,
        stock_price DOUBLE,
        shares_outstanding DOUBLE,
        scale VARCHAR,
        price_basis VARCHAR,
        corp_role VARCHAR
      )
    """)

    con.execute("""
      CREATE TABLE IF NOT EXISTS benchmark_map (
        corp_code VARCHAR,
        year INTEGER,
        benchmark_name_kr VARCHAR,
        bench_corp_code VARCHAR
      )
    """)

    con.register("tmp_market", all_market)
    con.execute("INSERT INTO market_data SELECT * FROM tmp_market")
    con.unregister("tmp_market")

    con.register("tmp_benchmap", bench_map_df)
    con.execute("INSERT INTO benchmark_map SELECT * FROM tmp_benchmap")
    con.unregister("tmp_benchmap")

    con.close()

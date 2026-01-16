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
    try:
        df = pd.read_csv(csv_path)

        # --- 필수 컬럼 최소 체크(원하면 더 강하게 검사 가능) ---
        required_cols = ["corp_code", "corp_name_kr", "stock_code", "year", "rcept_date"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"CSV에 필수 컬럼이 없습니다: {missing}")

        # --- 타입/키 정리 ---
        df["corp_code"] = df["corp_code"].astype(str).str.replace(".0", "", regex=False).str.zfill(8)

        # year 정리: NaN 제거 + int 강제
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df = df[df["year"].notna()].copy()
        df["year"] = df["year"].astype(int)

        # bench_corp_code 안전 처리 (csv에서 12345678.0 같은 형태 방지)
        if "bench_corp_code" in df.columns:
            bc = df["bench_corp_code"].astype(str).str.replace(".0", "", regex=False)
            bc = bc.replace({"nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
            df["bench_corp_code"] = bc.astype("string")

            mask = df["bench_corp_code"].notna()
            df.loc[mask, "bench_corp_code"] = df.loc[mask, "bench_corp_code"].str.zfill(8)
        else:
            df["bench_corp_code"] = pd.NA

        # --- market_data: target ---
        # corp_name_en, scale 컬럼이 없을 수도 있으니 안전하게 처리
        if "corp_name_en" not in df.columns:
            df["corp_name_en"] = pd.NA
        if "scale" not in df.columns:
            df["scale"] = pd.NA

        market_df = df[
            [
                "corp_code", "corp_name_kr", "corp_name_en",
                "stock_code", "year", "rcept_date",
            ]
        ].copy()

        # stock_price / shares_outstanding 가 없을 수도 있으니 있으면 붙이고 없으면 NA 채움
        if "stock_price" in df.columns:
            market_df["stock_price"] = df["stock_price"]
        else:
            market_df["stock_price"] = pd.NA

        if "shares_outstanding" in df.columns:
            market_df["shares_outstanding"] = df["shares_outstanding"]
        else:
            market_df["shares_outstanding"] = pd.NA

        market_df["scale"] = df["scale"]

        market_df = market_df.rename(columns={"rcept_date": "asof_date"})
        market_df["price_basis"] = "rcept_date"
        market_df["corp_role"] = "target"

        # --- market_data: benchmark도 market_data에 저장 ---
        bench_cols_min = ["bench_corp_code", "benchmark_name_kr", "bench_stock_code", "year", "bench_rcept_date"]
        has_bench_payload = all(c in df.columns for c in bench_cols_min)

        if has_bench_payload:
            bench_market_df = df[
                [
                    "bench_corp_code", "benchmark_name_kr",
                    "bench_stock_code", "year", "bench_rcept_date",
                ]
            ].copy()

            # 벤치 가격/주식수
            bench_market_df["stock_price"] = df["bench_stock_price"] if "bench_stock_price" in df.columns else pd.NA
            bench_market_df["shares_outstanding"] = df["bench_shares_outstanding"] if "bench_shares_outstanding" in df.columns else pd.NA

            bench_market_df = bench_market_df.rename(columns={
                "bench_corp_code": "corp_code",
                "benchmark_name_kr": "corp_name_kr",
                "bench_stock_code": "stock_code",
                "bench_rcept_date": "asof_date",
            })
            bench_market_df["corp_name_en"] = pd.NA
            bench_market_df["scale"] = pd.NA
            bench_market_df["price_basis"] = "rcept_date"
            bench_market_df["corp_role"] = "benchmark"

            # bench corp_code 없는 행 제거 (중요)
            bench_market_df = bench_market_df[bench_market_df["corp_code"].notna()].copy()
        else:
            bench_market_df = pd.DataFrame(columns=list(market_df.columns))

        all_market = pd.concat([market_df, bench_market_df], ignore_index=True)

        # --- benchmark_map ---
        if "benchmark_name_kr" not in df.columns:
            df["benchmark_name_kr"] = pd.NA

        bench_map_df = df[["corp_code", "year", "benchmark_name_kr", "bench_corp_code"]].copy()
        # bench_corp_code 없는 매핑은 제거(선택)
        bench_map_df = bench_map_df[bench_map_df["bench_corp_code"].notna()].copy()

        # --- 중복 제거(권장 키) ---
        all_market = all_market.drop_duplicates(subset=["corp_code", "year", "corp_role"], keep="last")
        bench_map_df = bench_map_df.drop_duplicates(subset=["corp_code", "year"], keep="last")

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

    finally:
        con.close()

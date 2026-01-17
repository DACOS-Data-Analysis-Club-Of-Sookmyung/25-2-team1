# src/sections/_common/builders/create_meta.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
import json
import duckdb

def build_meta(
    con: duckdb.DuckDBPyConnection,
    corp_name_kr: str,
    bsns_year: int,
) -> Dict[str, Any]:
    """
    섹션들이 공통으로 쓰는 메타:
    - corp_name, year, corp_code, stock_code, price, shares, scale, report_id 등
    """
    # market_data에서 target meta 우선
    row = con.execute("""
      SELECT corp_code, corp_name_kr, corp_name_en, stock_code, year,
             asof_date, stock_price, shares_outstanding, scale
      FROM market_data
      WHERE corp_name_kr=? AND year=? AND corp_role='target'
      LIMIT 1
    """, [corp_name_kr, int(bsns_year)]).fetchone()

    if row is None:
        # fallback
        row = con.execute("""
          SELECT corp_code, corp_name_kr, corp_name_en, stock_code, year,
                 asof_date, stock_price, shares_outstanding, scale
          FROM market_data
          WHERE corp_name_kr=? AND year=?
          LIMIT 1
        """, [corp_name_kr, int(bsns_year)]).fetchone()

    if row is None:
        raise ValueError(f"market_data에서 {corp_name_kr}, {bsns_year}를 찾지 못함")

    (corp_code, name_kr, name_en, stock_code, year,
     asof_date, stock_price, shares_outstanding, scale) = row

    # reports에서 report_id 찾아오기 (이미 ingest 되어있다는 전제)
    rep = con.execute("""
      SELECT report_id, rcept_no
      FROM reports
      WHERE corp_code=? AND bsns_year=?
      ORDER BY try_cast(rcept_no AS BIGINT) DESC
      LIMIT 1
    """, [str(corp_code), int(bsns_year)]).fetchone()

    report_id = rep[0] if rep else None
    rcept_no = rep[1] if rep else None

    return {
        "corp_name": str(name_kr),
        "corp_name_kr": str(name_kr),
        "corp_name_en": str(name_en) if name_en is not None else None,
        "corp_code": str(corp_code),
        "stock_code": str(stock_code) if stock_code is not None else None,
        "bsns_year": int(year),
        "asof_date": int(asof_date) if asof_date is not None else None,
        "stock_price": float(stock_price) if stock_price is not None else None,
        "shares_outstanding": float(shares_outstanding) if shares_outstanding is not None else None,
        "scale": str(scale) if scale is not None else None,
        "report_id": str(report_id) if report_id is not None else None,
        "rcept_no": str(rcept_no) if rcept_no is not None else None,
    }

def save_meta_json(meta: Dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

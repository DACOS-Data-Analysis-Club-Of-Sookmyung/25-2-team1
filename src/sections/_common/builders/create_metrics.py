# src/sections/_common/builders/create_metrics.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
import json
import duckdb

def _pick_latest_periods(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    statement_type: str,
    mode: str,  # "latest_two" | "current_only"
) -> List[str]:
    rows = con.execute("""
      SELECT DISTINCT period_end
      FROM fs_facts f
      JOIN fs_line_items li ON li.line_item_id=f.line_item_id
      WHERE f.report_id=? AND li.statement_type=?
        AND f.period_end IS NOT NULL
      ORDER BY period_end DESC
      LIMIT 2
    """, [report_id, statement_type]).fetchall()
    periods = [str(r[0]) for r in rows if r and r[0] is not None]
    if mode == "current_only":
        return periods[:1]
    return periods[:2]  # latest_two

def build_metrics_for_section(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    metrics_spec: Any,  # dict 또는 list[dict]
) -> Dict[str, Any]:
    """
    metrics_spec 예시:
    {
      "statement_type": "BS",
      "periods": "latest_two",  # "latest_two" | "current_only"
      "keys": [
        {"key":"TOTAL_LIABILITIES", "label_like":"부채총계"},
        {"key":"CURRENT_LIABILITIES", "label_like":"유동부채"}
      ]
    }
    """
    # 없으면 빈 metrics
    if not metrics_spec:
        return {"report_id": report_id, "rows": []}

    # list면 첫 dict만 사용(필요하면 나중에 multi-group 확장)
    if isinstance(metrics_spec, list):
        metrics_spec = metrics_spec[0] if len(metrics_spec) > 0 else None
    if not isinstance(metrics_spec, dict):
        return {"report_id": report_id, "rows": []}

    stype = metrics_spec.get("statement_type")
    mode = (metrics_spec.get("periods") or "latest_two").strip()
    items = metrics_spec.get("keys") or []
    if not stype or not items:
        return {"report_id": report_id, "rows": []}

    periods = _pick_latest_periods(con, report_id, stype, mode=mode)
    p_cur = periods[0] if len(periods) > 0 else None
    p_prev = periods[1] if len(periods) > 1 else None

    out_rows = []
    for it in items:
        key = it.get("key")
        label_like = it.get("label_like")
        ifrs_code = it.get("ifrs_code")

        if not key:
            continue

        # line_item_id 찾기: ifrs_code 우선, 없으면 label_like
        if ifrs_code:
            li = con.execute("""
              SELECT line_item_id, label_ko, label_clean
              FROM fs_line_items
              WHERE statement_type=? AND ifrs_code=?
              LIMIT 1
            """, [stype, ifrs_code]).fetchone()
        else:
            li = con.execute("""
              SELECT line_item_id, label_ko, label_clean
              FROM fs_line_items
              WHERE statement_type=? AND label_clean LIKE ?
              ORDER BY length(label_clean) ASC
              LIMIT 1
            """, [stype, f"%{label_like}%"]).fetchone()

        if not li:
            out_rows.append({
                "key": key,
                "found": False,
                "line_item_id": None,
                "label_ko": None,
                "label_clean": None,
                "periods": [],
            })
            continue

        line_item_id, label_ko, label_clean = li

        # facts: 당기/전기 값
        facts = con.execute("""
          SELECT period_end, fiscal_year, value, unit_multiplier, currency, table_id, row_idx, col_idx
          FROM fs_facts
          WHERE report_id=? AND line_item_id=?
            AND (
              (? IS NULL OR period_end=?) OR
              (? IS NULL OR period_end=?)
            )
          ORDER BY period_end DESC
        """, [report_id, line_item_id, p_cur, p_cur, p_prev, p_prev]).fetchall()

        periods_out = []
        for (period_end, fiscal_year, value, unit_multiplier, currency, table_id, row_idx, col_idx) in facts:
            periods_out.append({
                "period_end": str(period_end) if period_end is not None else None,
                "fiscal_year": int(fiscal_year) if fiscal_year is not None else None,
                "value": float(value) if value is not None else None,
                "unit_multiplier": int(unit_multiplier) if unit_multiplier is not None else None,
                "currency": currency,
                "trace": {"table_id": table_id, "row_idx": row_idx, "col_idx": col_idx},
            })

        out_rows.append({
            "key": key,
            "found": True,
            "line_item_id": str(line_item_id),
            "label_ko": str(label_ko) if label_ko is not None else None,
            "label_clean": str(label_clean) if label_clean is not None else None,
            "periods": periods_out,
        })

    return {"report_id": report_id, "statement_type": stype, "rows": out_rows}

def save_metrics_json(obj: Dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

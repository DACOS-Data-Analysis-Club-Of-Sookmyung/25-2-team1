# prompts/common/builders/create_metrics.py
# metrics_spec의 key들을 fact_metrics.metric_key로 조회
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional
import json

import duckdb


def _report_meta(con: duckdb.DuckDBPyConnection, report_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT corp_code, bsns_year
        FROM reports
        WHERE report_id=?
        LIMIT 1
        """,
        [report_id],
    ).fetchone()
    if not row:
        return {"corp_code": None, "bsns_year": None}
    return {"corp_code": str(row[0]), "bsns_year": int(row[1])}


def _normalize_metrics_spec(metrics_spec: Any) -> List[str]:
    """Accepts list[str] (new) or legacy dict/list[dict] (old) and returns metric keys."""
    if not metrics_spec:
        return []

    # ✅ new format: ["REVENUE", "COGS", ...]
    if isinstance(metrics_spec, list) and (len(metrics_spec) == 0 or isinstance(metrics_spec[0], str)):
        return [str(x) for x in metrics_spec if x]

    # legacy: list[dict] -> first dict
    if isinstance(metrics_spec, list):
        metrics_spec = metrics_spec[0] if metrics_spec else None

    # legacy dict example:
    # {"keys": [{"key": "TOTAL_ASSETS"}, ...]}
    if isinstance(metrics_spec, dict):
        keys = metrics_spec.get("keys") or []
        out = []
        for it in keys:
            if isinstance(it, dict) and it.get("key"):
                out.append(str(it["key"]))
            elif isinstance(it, str):
                out.append(it)
        return out

    return []


def build_metrics_for_section(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    metrics_spec: Any,
) -> Dict[str, Any]:
    """Build metrics JSON rows from fact_metrics.

    New expected metrics_spec:
      ["REVENUE", "COGS", "TOTAL_ASSETS", "asset_turnover", ...]

    Output row shape (example):
      {
        "metric_key": "REVENUE",
        "metric_name_ko": "매출액",
        "metric_type": "raw",
        "value": 123,
        "value_prev": 100,
        "yoy_abs": 23,
        "yoy_pct": 0.23,
        "unit": "KRW",
        "benchmark_corp_code": "000660",
        "benchmark_value": 456,
        "benchmark_improved": false
      }

    Notes:
    - Evidence는 create_evidence에서 fs_facts/text_chunks로 따로 붙임.
    """
    keys = _normalize_metrics_spec(metrics_spec)
    if not keys:
        return {"report_id": report_id, "rows": []}

    meta = _report_meta(con, report_id)
    corp_code = meta.get("corp_code")
    bsns_year = meta.get("bsns_year")

    if not corp_code or bsns_year is None:
        # report meta missing
        return {"report_id": report_id, "rows": []}

    # fact_metrics가 없으면 빈 metrics
    has_fact_metrics = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema='main' AND table_name='fact_metrics'
        LIMIT 1
        """
    ).fetchone() is not None
    if not has_fact_metrics:
        return {"report_id": report_id, "rows": []}

    q = """
      SELECT
        metric_key,
        metric_name_ko,
        metric_type,
        value,
        value_prev,
        yoy_abs,
        yoy_pct,
        unit,
        benchmark_corp_code,
        benchmark_value,
        benchmark_improved
      FROM fact_metrics
      WHERE corp_code=?
        AND bsns_year=?
        AND metric_key IN (SELECT UNNEST(?))
    """
    rows = con.execute(q, [corp_code, int(bsns_year), keys]).fetchall()

    # build map for stable ordering and missing keys
    found: Dict[str, Dict[str, Any]] = {}
    for (
        metric_key,
        metric_name_ko,
        metric_type,
        value,
        value_prev,
        yoy_abs,
        yoy_pct,
        unit,
        benchmark_corp_code,
        benchmark_value,
        benchmark_improved,
    ) in rows:
        mk = str(metric_key)
        found[mk] = {
            "metric_key": mk,
            "metric_name_ko": str(metric_name_ko) if metric_name_ko is not None else None,
            "metric_type": str(metric_type) if metric_type is not None else None,
            "value": float(value) if value is not None else None,
            "value_prev": float(value_prev) if value_prev is not None else None,
            "yoy_abs": float(yoy_abs) if yoy_abs is not None else None,
            "yoy_pct": float(yoy_pct) if yoy_pct is not None else None,
            "unit": str(unit) if unit is not None else None,
            "benchmark_corp_code": str(benchmark_corp_code) if benchmark_corp_code is not None else None,
            "benchmark_value": float(benchmark_value) if benchmark_value is not None else None,
            "benchmark_improved": bool(benchmark_improved) if benchmark_improved is not None else None,
        }

    out_rows: List[Dict[str, Any]] = []
    for k in keys:
        if k in found:
            out_rows.append(found[k])
        else:
            out_rows.append(
                {
                    "metric_key": str(k),
                    "metric_name_ko": None,
                    "metric_type": None,
                    "value": None,
                    "value_prev": None,
                    "yoy_abs": None,
                    "yoy_pct": None,
                    "unit": None,
                    "benchmark_corp_code": None,
                    "benchmark_value": None,
                    "benchmark_improved": None,
                    "missing": True,
                }
            )

    return {
        "report_id": report_id,
        "corp_code": corp_code,
        "bsns_year": int(bsns_year),
        "rows": out_rows,
    }


def save_metrics_json(obj: Dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

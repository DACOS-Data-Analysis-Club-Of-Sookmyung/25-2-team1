from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY, render_T_SIMPLE, render_T3_TRACE

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir, spec_id=spec["id"], allow_missing_evidence=True)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    bs_assets_table = render_T1_YOY(metric_rows, ["TOTAL_ASSETS", "CURRENT_ASSETS", "CASH_EQ"])
    bs_liabilities_table = render_T1_YOY(metric_rows, ["TOTAL_LIABILITIES", "CURRENT_LIABILITIES", "SHORT_TERM_DEBT"])
    bs_equity_table = render_T1_YOY(metric_rows, ["EQUITY"])

    financial_health_table = render_T_SIMPLE(
        metric_rows,
        ["current_ratio", "quick_ratio", "interest_coverage"],
    )

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bs_assets_table": bs_assets_table,
        "bs_liabilities_table": bs_liabilities_table,
        "bs_equity_table": bs_equity_table,
        "financial_health_table": financial_health_table,
        "key_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

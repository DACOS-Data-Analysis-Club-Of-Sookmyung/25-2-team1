from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY, render_T_SIMPLE, render_T3_TRACE

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(
        workdir,
        metrics_path=spec.get("metrics_path"),
        evidence_path=spec.get("evidence_path"),
    )
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    bs_equity_table = render_T1_YOY(
        metric_rows,
        ["EQUITY", "CAPITAL_STOCK", "RETAINED_EARNINGS", "NON_CONTROLLING_INTEREST"],
    )

    equity_ratio_table = render_T_SIMPLE(metric_rows, ["roe"])

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bs_equity_table": bs_equity_table,
        "equity_ratio_table": equity_ratio_table,
        "equity_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY

def build_context(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(
        workdir,
        metrics_path=spec.get("metrics_path"),
        evidence_path=spec.get("evidence_path"),
    )
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    is_profit_table = render_T1_YOY(
        metric_rows,
        ["OP_PROFIT", "PRE_TAX_INCOME", "TAX_EXP", "NET_INCOME"],
    )

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "is_profit_table": is_profit_table,
        "profit_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

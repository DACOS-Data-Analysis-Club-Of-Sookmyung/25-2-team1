from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    financing_metrics_table = render_T1_YOY(
        metric_rows,
        ["SHORT_TERM_DEBT", "LONG_TERM_DEBT", "FCF_FIN"],
    )

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "financing_metrics_table": financing_metrics_table,
        "financing_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

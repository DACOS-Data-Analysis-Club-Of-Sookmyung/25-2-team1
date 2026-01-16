from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY, render_T3_TRACE

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    bs_liabilities_table = render_T1_YOY(
        metric_rows,
        [
            "TOTAL_LIABILITIES",
            "CURRENT_LIABILITIES",
            "NON_CURRENT_LIABILITIES",
            "ACCOUNTS_PAYABLE",
            "SHORT_TERM_DEBT",
            "LONG_TERM_DEBT",
        ],
    )

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bs_liabilities_table": bs_liabilities_table,
        "liabilities_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

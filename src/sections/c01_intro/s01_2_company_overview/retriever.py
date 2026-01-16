from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

from src.sections._common.io import load_inputs, pack_evidence

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(
        workdir,
        metrics_path=spec.get("metrics_path"),
        evidence_path=spec.get("evidence_path"),
    )
    meta = inputs["meta"]
    evidence_rows = inputs["evidence_rows"]

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "company_overview_evidence": pack_evidence(
            evidence_rows,
            topk=len(evidence_rows),
            include_tables=False
        ),
    }

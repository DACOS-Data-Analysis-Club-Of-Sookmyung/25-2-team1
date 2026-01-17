from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

from src.sections._common.io import load_inputs

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir, spec_id=spec["id"], allow_missing_evidence=True)
    meta = inputs["meta"]

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
    }

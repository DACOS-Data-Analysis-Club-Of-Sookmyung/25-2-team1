from __future__ import annotations
import json
from pathlib import Path
from typing import Dict, Any

from src.sections._common.io import pack_evidence

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    workdir = Path(workdir)

    meta = json.loads((workdir / "meta" / "meta.json").read_text(encoding="utf-8"))

    evidence_rows = []
    ev_path = workdir / "evidence" / "s01_2_evidence.json"
    if ev_path.exists():
        ev_obj = json.loads(ev_path.read_text(encoding="utf-8"))
        evidence_rows = ev_obj.get("rows") or []

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or "",
        "bsns_year": meta.get("bsns_year"),
        "company_overview_evidence": pack_evidence(
            evidence_rows,
            topk=len(evidence_rows),
            include_tables=False
        ),
    }

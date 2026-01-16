# src/sections/_common/io.py
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List
import json

def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def load_inputs(workdir: Path) -> Dict[str, Any]:
    meta = read_json(workdir / "meta.json")
    metrics = read_json(workdir / "metrics.json")
    evidence = read_json(workdir / "evidence.json")

    metric_rows = {r["metric_key"]: r for r in metrics.get("rows", [])}
    evidence_rows = evidence.get("rows", [])

    return {
        "meta": meta,
        "metrics": metrics,
        "metric_rows": metric_rows,
        "evidence": evidence,
        "evidence_rows": evidence_rows
    }

def pack_evidence(evidence_rows: List[Dict[str, Any]], topk: int, include_tables: bool) -> str:
    out = []
    for c in evidence_rows[:topk]:
        out.append(f"- {c.get('text','')} (근거: note_no={c.get('note_no')}, section_code={c.get('section_code')}, chunk_id={c.get('chunk_id')})")
        if include_tables:
            for t in c.get("table_refs", [])[:3]:
                out.append(f"  - [TABLE] {t.get('caption','')}\n{t.get('table_md','')}")
    return "\n".join(out)

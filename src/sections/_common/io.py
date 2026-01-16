# src/sections/_common/io.py
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))

def load_inputs(
    workdir: Path,
    metrics_path: Optional[str] = None,
    evidence_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    - meta.json은 항상 workdir/meta.json에서 읽음 (공통)
    - metrics/evidence는
      1) spec에서 받은 상대경로(metrics_path/evidence_path)가 있으면 그걸 사용
      2) 없으면 기존 호환을 위해 workdir/metrics.json, workdir/evidence.json을 fallback
    """
    meta = read_json(workdir / "meta.json")

    # --- metrics ---
    if metrics_path:
        mpath = (workdir / metrics_path)
    else:
        mpath = (workdir / "metrics.json")
    metrics = read_json(mpath)

    # --- evidence ---
    if evidence_path:
        epath = (workdir / evidence_path)
    else:
        epath = (workdir / "evidence.json")
    evidence = read_json(epath)

    metric_rows = {r["metric_key"]: r for r in metrics.get("rows", [])}
    evidence_rows = evidence.get("rows", [])

    return {
        "meta": meta,
        "metrics": metrics,
        "metric_rows": metric_rows,
        "evidence": evidence,
        "evidence_rows": evidence_rows,
        "_metrics_file": str(mpath),
        "_evidence_file": str(epath),
    }

def pack_evidence(evidence_rows: List[Dict[str, Any]], topk: int, include_tables: bool) -> str:
    out = []
    for c in evidence_rows[:topk]:
        out.append(
            f"- {c.get('text','')} "
            f"(근거: note_no={c.get('note_no')}, section_code={c.get('section_code')}, chunk_id={c.get('chunk_id')})"
        )
        if include_tables:
            for t in c.get("table_refs", [])[:3]:
                out.append(f"  - [TABLE] {t.get('caption','')}\n{t.get('table_md','')}")
    return "\n".join(out)

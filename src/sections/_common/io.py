# src/sections/_common/io.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def _resolve_under_workdir(workdir: Path, p: str | Path) -> Path:
    p = Path(p)
    return p if p.is_absolute() else (workdir / p)


def _default_section_stem(spec_id: str) -> str:
    """
    spec_id 예: 'c04.s04_1' -> 's04_1'
    """
    if "." in spec_id:
        return spec_id.split(".", 1)[1]
    return spec_id


def _pick_existing(cands: List[Path]) -> Optional[Path]:
    for p in cands:
        if p.exists():
            return p
    return None


def load_inputs(
    workdir: Path,
    *,
    spec_id: Optional[str] = None,
    metrics_path: Optional[str | Path] = None,
    evidence_path: Optional[str | Path] = None,
    allow_missing_evidence: bool = False,
) -> Dict[str, Any]:
    """
    팀 규칙/정책:
    - meta: workdir/meta/meta.json (필수)
    - metrics/evidence:
      1) path가 주어지면(workdir 기준 상대경로 가능) 그걸 사용
      2) path가 None이면 spec_id 기반 기본 경로로 자동 선택

    기본 파일명 규칙:
      metrics:  workdir/metrics/{stem}_metrics.json   (stem = 's04_1')
      evidence: workdir/evidence/{stem}_evidence.json
    fallback:
      workdir/metrics/{spec_id}_metrics.json 등도 시도

    반환은 기존 코드 호환을 위해 metrics/evidence 키도 함께 제공한다.
    """
    workdir = Path(workdir).resolve()

    # --------
    # meta (required)
    # --------
    meta_path = workdir / "meta" / "meta.json"
    if not meta_path.exists():
        raise FileNotFoundError(f"meta.json not found: {meta_path}")
    meta = _read_json(meta_path)

    # --------
    # metrics (required)
    # --------
    if metrics_path is not None:
        mpath = _resolve_under_workdir(workdir, metrics_path)
        if not mpath.exists():
            raise FileNotFoundError(f"metrics_path not found: {mpath}")
    else:
        if not spec_id:
            raise ValueError("metrics_path is None. Provide spec_id to auto-resolve metrics file.")
        stem = _default_section_stem(spec_id)
        m_cands = [
            workdir / "metrics" / f"{stem}_metrics.json",
            workdir / "metrics" / f"{spec_id}_metrics.json",
            workdir / "metrics" / f"{stem}.json",
            workdir / "metrics" / f"{spec_id}.json",
        ]
        mpath = _pick_existing(m_cands)
        if mpath is None:
            raise FileNotFoundError(
                "metrics file not found. Tried:\n- " + "\n- ".join(str(x) for x in m_cands)
            )

    metrics_obj = _read_json(mpath)
    metric_rows = metrics_obj.get("rows") or []
    if not isinstance(metric_rows, list):
        raise TypeError(f"metrics.rows must be a list: {mpath}")

    # --------
    # evidence (required unless allow_missing_evidence=True)
    # --------
    if evidence_path is not None:
        epath = _resolve_under_workdir(workdir, evidence_path)
        if not epath.exists():
            if allow_missing_evidence:
                epath = None
            else:
                raise FileNotFoundError(f"evidence_path not found: {epath}")
    else:
        if not spec_id:
            raise ValueError("evidence_path is None. Provide spec_id to auto-resolve evidence file.")
        stem = _default_section_stem(spec_id)
        e_cands = [
            workdir / "evidence" / f"{stem}_evidence.json",
            workdir / "evidence" / f"{spec_id}_evidence.json",
            workdir / "evidence" / f"{stem}.json",
            workdir / "evidence" / f"{spec_id}.json",
        ]
        epath = _pick_existing(e_cands)
        if epath is None and not allow_missing_evidence:
            raise FileNotFoundError(
                "evidence file not found. Tried:\n- " + "\n- ".join(str(x) for x in e_cands)
            )

    if epath is None:
        evidence_obj = {"rows": []}
        evidence_rows = []
        epath_str = ""
    else:
        evidence_obj = _read_json(epath)
        evidence_rows = evidence_obj.get("rows") or []
        if not isinstance(evidence_rows, list):
            raise TypeError(f"evidence.rows must be a list: {epath}")
        epath_str = str(epath)

    # --------
    # return (compat-friendly)
    # --------
    return {
        "meta": meta,

        "metrics": metrics_obj,
        "evidence": evidence_obj,

        "metrics_obj": metrics_obj,
        "evidence_obj": evidence_obj,

        "metric_rows": metric_rows,
        "evidence_rows": evidence_rows,

        "metrics_path_resolved": str(mpath),
        "evidence_path_resolved": epath_str,
        "meta_path_resolved": str(meta_path),
    }


def pack_evidence(
    evidence_rows: List[Dict[str, Any]],
    topk: int,
    include_tables: bool,
) -> str:
    out: List[str] = []
    for c in evidence_rows[:topk]:
        out.append(
            f"- {c.get('text','')} "
            f"(근거: note_no={c.get('note_no')}, section_code={c.get('section_code')}, chunk_id={c.get('chunk_id')})"
        )
        if include_tables:
            for t in (c.get("table_refs") or [])[:3]:
                out.append(f"  - [TABLE] {t.get('caption','')}\n{t.get('table_md','')}")
    return "\n".join(out)

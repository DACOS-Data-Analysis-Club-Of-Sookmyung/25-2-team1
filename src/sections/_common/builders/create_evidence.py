# src/sections/_common/builders/create_evidence.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List, Set, Optional
import json
import duckdb

def _report_meta_for_evidence(con: duckdb.DuckDBPyConnection, report_id: str) -> Dict[str, Any]:
    row = con.execute("""
      SELECT corp_code, bsns_year
      FROM reports
      WHERE report_id=?
      LIMIT 1
    """, [report_id]).fetchone()
    if not row:
        return {"corp_code": None, "bsns_year": None}
    return {"corp_code": str(row[0]), "bsns_year": int(row[1])}

def _build_evidence_notes_by_metrics(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    metrics_json: Any,  
    topk_chunks_per_note: int,
) -> Dict[str, Any]:
    line_item_ids = []

    # ✅ [ADD] metrics_json 방어: dict가 아니면 rows를 안전하게 처리
    rows_list = []
    if isinstance(metrics_json, dict):
        rows_list = metrics_json.get("rows") or []
    elif isinstance(metrics_json, list):
        # 혹시 rows만 넘어온 경우
        rows_list = metrics_json
    else:
        rows_list = []

    for r in rows_list:
        if isinstance(r, dict) and r.get("found") and r.get("line_item_id"):
            line_item_ids.append(r["line_item_id"])

    if not line_item_ids:
        return {"rows": [], "note_nos": []}

    rows = con.execute("""
      SELECT DISTINCT note_no, note_section_id, line_item_id, confidence
      FROM note_links
      WHERE report_id=? AND line_item_id IN (SELECT UNNEST(?))
      ORDER BY confidence DESC
    """, [report_id, line_item_ids]).fetchall()

    note_nos: Set[int] = set()
    for (note_no, _note_section_id, _line_item_id, _conf) in rows:
        if note_no is not None:
            note_nos.add(int(note_no))

    if not note_nos:
        return {"rows": [], "note_nos": []}

    out_rows = []
    for no in sorted(note_nos):
        chunks = con.execute("""
          SELECT chunk_id, section_code, section_type, note_no, chunk_idx, text
          FROM rag_text_chunks
          WHERE report_id=? AND section_type='notes' AND note_no=?
          ORDER BY chunk_idx ASC
          LIMIT ?
        """, [report_id, int(no), int(topk_chunks_per_note)]).fetchall()

        for (chunk_id, section_code, section_type, note_no, chunk_idx, text) in chunks:
            out_rows.append({
                "chunk_id": str(chunk_id),
                "section_code": str(section_code),
                "section_type": str(section_type),
                "note_no": int(note_no) if note_no is not None else None,
                "chunk_idx": int(chunk_idx),
                "text": text,
                "table_refs": [],   # 지금은 비워두고, 필요하면 나중에 [[TABLE:...]] 파싱해서 채우기
            })

    return {"rows": out_rows, "note_nos": sorted(note_nos)}

def _build_evidence_biz_all(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    section_code_prefixes: Optional[List[str]] = None,  # 예: ["I-", "II-"]
) -> Dict[str, Any]:
    if not section_code_prefixes:
        section_code_prefixes = ["I-", "II-"]

    # rag_text_chunks에서 biz 섹션 전체를 가져옴
    # - ingest에서 I/II를 section_type='biz'로 저장했고
    # - section_code는 I-*, II-* 형태
    cond = " OR ".join(["section_code LIKE ?"] * len(section_code_prefixes))
    params = [report_id] + [f"{p}%" for p in section_code_prefixes]

    q = f"""
      SELECT chunk_id, section_code, section_type, note_no, chunk_idx, text
      FROM rag_text_chunks
      WHERE report_id=? AND section_type='biz'
        AND ({cond})
      ORDER BY section_code ASC, chunk_idx ASC
    """
    chunks = con.execute(q, params).fetchall()

    out_rows = []
    for (chunk_id, section_code, section_type, note_no, chunk_idx, text) in chunks:
        out_rows.append({
            "chunk_id": str(chunk_id),
            "section_code": str(section_code),
            "section_type": str(section_type),
            "note_no": None,
            "chunk_idx": int(chunk_idx),
            "text": text,
            "table_refs": [],
        })

    return {"rows": out_rows, "note_nos": []}

def build_evidence_for_section(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    evidence_spec: Any,                 # dict 또는 list[dict] 허용
    metrics_json: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    evidence_spec 예시:
    - {"type":"biz", "section_code_prefixes":["I-","II-"]}
    - {"type":"notes_by_metrics", "topk_chunks_per_note": 5}
    """
    meta = _report_meta_for_evidence(con, report_id)

    # normalize: list -> first element
    if isinstance(evidence_spec, list):
        evidence_spec = evidence_spec[0] if len(evidence_spec) > 0 else None
    if not isinstance(evidence_spec, dict):
        return {"corp_code": meta["corp_code"], "bsns_year": meta["bsns_year"], "report_id": report_id, "rows": []}

    etype = (evidence_spec.get("type") or "").strip()

    if etype == "biz":
        prefixes = evidence_spec.get("section_code_prefixes") or ["I-", "II-"]
        r = _build_evidence_biz_all(con, report_id, section_code_prefixes=list(prefixes))
        return {"corp_code": meta["corp_code"], "bsns_year": meta["bsns_year"], "report_id": report_id, "rows": r["rows"]}

    # default: notes_by_metrics
    topk = int(evidence_spec.get("topk_chunks_per_note") or 5)
    r = _build_evidence_notes_by_metrics(con, report_id, metrics_json or {}, topk_chunks_per_note=topk)
    return {"corp_code": meta["corp_code"], "bsns_year": meta["bsns_year"], "report_id": report_id, "rows": r["rows"]}

def save_evidence_json(obj: Dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

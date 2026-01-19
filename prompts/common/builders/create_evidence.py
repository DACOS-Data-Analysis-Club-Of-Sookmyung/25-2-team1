# prompts/common/builders/create_evidence.py
# evidence_spec은 biz일 때만 직접 텍스트 가져오기
# 그 외 주석 텍스트 evidence는 metrics_spec 기반으로 자동 생성
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set
import json
import re
import duckdb

# metric_key -> (statement_type, label_like)
# NOTE: 테이블 추가 없이 가려면 최소 이 매핑은 코드에 필요함.
FS_MAP: Dict[str, Dict[str, str]] = {
    "REVENUE": {"statement_type": "IS", "label_like": "매출"},        # 가능하면 "매출액"으로 더 좁혀
    "COGS": {"statement_type": "IS", "label_like": "매출원가"},
    "TOTAL_ASSETS": {"statement_type": "BS", "label_like": "자산총계"},
    "INVENTORIES": {"statement_type": "BS", "label_like": "재고"},
    "AR": {"statement_type": "BS", "label_like": "매출채권"},
}

# ratio/derived는 입력(raw)들의 주석을 묶어서 제공
RATIO_REQ: Dict[str, List[str]] = {
    "asset_turnover": ["REVENUE", "TOTAL_ASSETS"],
    "inventory_turnover": ["COGS", "INVENTORIES"],
    "ar_turnover": ["REVENUE", "AR"],
}

_TABLE_PAT = re.compile(r"\[\[TABLE:([a-f0-9]{8,64})\]\]")


def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema='main' AND table_name=?
        LIMIT 1
        """,
        [name],
    ).fetchone()
    return row is not None


def _text_chunk_table(con: duckdb.DuckDBPyConnection) -> str:
    # 파이프라인에 따라 text_chunks 또는 rag_text_chunks
    if _table_exists(con, "text_chunks"):
        return "text_chunks"
    if _table_exists(con, "rag_text_chunks"):
        return "rag_text_chunks"
    return "text_chunks"


def _report_meta(con: duckdb.DuckDBPyConnection, report_id: str) -> Dict[str, Any]:
    row = con.execute(
        """
        SELECT corp_code, bsns_year
        FROM reports
        WHERE report_id=?
        LIMIT 1
        """,
        [report_id],
    ).fetchone()
    if not row:
        return {"corp_code": None, "bsns_year": None}
    return {"corp_code": str(row[0]), "bsns_year": int(row[1])}


def _extract_table_refs(con: duckdb.DuckDBPyConnection, text: str) -> List[Dict[str, Any]]:
    if not text:
        return []
    table_ids = list(dict.fromkeys(_TABLE_PAT.findall(text)))
    if not table_ids:
        return []
    # tables 테이블이 없을 수도 있으니 caption은 optional
    refs = [{"table_id": tid, "caption": None} for tid in table_ids]
    if _table_exists(con, "tables"):
        for r in refs:
            cap = con.execute(
                "SELECT table_title FROM tables WHERE table_id=? LIMIT 1",
                [r["table_id"]],
            ).fetchone()
            if cap:
                r["caption"] = cap[0]
    return refs


def _find_line_item_id(con: duckdb.DuckDBPyConnection, report_id: str, metric_key: str) -> Optional[Dict[str, Any]]:
    m = FS_MAP.get(metric_key)
    if not m:
        return None

    st = m["statement_type"]
    like = m["label_like"]

    # report_id에 실제 facts가 존재하는 line_item만 선택 (중요!)
    row = con.execute(
        """
        SELECT li.line_item_id, li.label_ko, li.label_clean
        FROM fs_line_items li
        JOIN fs_facts f ON f.line_item_id = li.line_item_id
        WHERE f.report_id=?
          AND li.statement_type=?
          AND li.label_clean LIKE ?
        ORDER BY length(li.label_clean) ASC
        LIMIT 1
        """,
        [report_id, st, f"%{like}%"],
    ).fetchone()

    if not row:
        return None

    return {
        "line_item_id": str(row[0]),
        "statement_type": st,
        "label_ko": str(row[1]) if row[1] is not None else None,
        "label_clean": str(row[2]) if row[2] is not None else None,
    }


def _note_nos_from_note_links(con: duckdb.DuckDBPyConnection, report_id: str, line_item_id: str, max_notes: int) -> List[int]:
    if not _table_exists(con, "note_links"):
        return []
    rows = con.execute(
        """
        SELECT note_no
        FROM note_links
        WHERE report_id=? AND line_item_id=? AND note_no IS NOT NULL
        ORDER BY confidence DESC
        LIMIT ?
        """,
        [report_id, line_item_id, int(max_notes)],
    ).fetchall()
    return [int(r[0]) for r in rows if r and r[0] is not None]


def _fetch_note_chunks(con: duckdb.DuckDBPyConnection, report_id: str, note_no: int, topk_chunks_per_note: int) -> List[Dict[str, Any]]:
    tbl = _text_chunk_table(con)
    rows = con.execute(
        f"""
        SELECT chunk_id, section_code, section_type, note_no, chunk_idx, text
        FROM {tbl}
        WHERE report_id=? AND section_type='notes' AND note_no=?
        ORDER BY chunk_idx ASC
        LIMIT ?
        """,
        [report_id, int(note_no), int(topk_chunks_per_note)],
    ).fetchall()

    out = []
    for (chunk_id, section_code, section_type, note_no2, chunk_idx, text) in rows:
        out.append(
            {
                "chunk_id": str(chunk_id),
                "section_code": str(section_code) if section_code is not None else None,
                "section_type": str(section_type) if section_type is not None else None,
                "note_no": int(note_no2) if note_no2 is not None else None,
                "chunk_idx": int(chunk_idx) if chunk_idx is not None else None,
                "text": text,
                "table_refs": _extract_table_refs(con, text),
            }
        )
    return out


def _build_notes_evidence_for_metric(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    metric_key: str,
    max_notes: int,
    topk_chunks_per_note: int,
) -> Dict[str, Any]:
    # ratio/derived: input raw들의 note evidence를 묶어준다
    if metric_key in RATIO_REQ:
        inputs = RATIO_REQ[metric_key]
        return {
            "metric_key": metric_key,
            "type": "ratio",
            "inputs": inputs,
            "input_evidence": {
                ik: _build_notes_evidence_for_metric(con, report_id, ik, max_notes, topk_chunks_per_note)
                for ik in inputs
            },
        }

    # raw: metric_key -> line_item_id -> note_no -> chunks
    li = _find_line_item_id(con, report_id, metric_key)
    if not li:
        return {
            "metric_key": metric_key,
            "type": "raw",
            "line_item": None,
            "notes": [],
            "missing_mapping": True,
        }

    note_nos = _note_nos_from_note_links(con, report_id, li["line_item_id"], max_notes=max_notes)

    notes_out = []
    for no in note_nos:
        chunks = _fetch_note_chunks(con, report_id, no, topk_chunks_per_note=topk_chunks_per_note)
        if chunks:
            notes_out.append({"note_no": int(no), "chunks": chunks})

    return {
        "metric_key": metric_key,
        "type": "raw",
        "line_item": li,
        "notes": notes_out,
    }


def _build_biz_evidence(con: duckdb.DuckDBPyConnection, report_id: str, topk_chunks: int) -> Dict[str, Any]:
    tbl = _text_chunk_table(con)
    rows = con.execute(
        f"""
        SELECT chunk_id, section_id, section_code, section_type, chunk_idx, text
        FROM {tbl}
        WHERE report_id=? AND section_type='biz'
        ORDER BY section_code, chunk_idx
        LIMIT ?
        """,
        [report_id, int(topk_chunks)],
    ).fetchall()

    out = []
    for (chunk_id, section_id, section_code, section_type, chunk_idx, text) in rows:
        out.append(
            {
                "chunk_id": str(chunk_id),
                "section_id": str(section_id) if section_id is not None else None,
                "section_code": str(section_code) if section_code is not None else None,
                "section_type": str(section_type) if section_type is not None else None,
                "chunk_idx": int(chunk_idx) if chunk_idx is not None else None,
                "text": text,
                "table_refs": _extract_table_refs(con, text),
            }
        )
    return {"type": "biz", "chunks": out}


def build_evidence_for_section(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    evidence_spec: Any | None,
    metrics_json: Dict[str, Any] | None,
    *,
    max_notes: int = 8,
    topk_chunks_per_note: int = 5,
    topk_biz_chunks: int = 80,
) -> Dict[str, Any]:
    """
    ✅ 요구사항 반영:
    - evidence_spec은 "biz"일 때만 사용
    - 그 외에는 evidence_spec이 있어도 무시하고, metrics_json 기반으로 주석 evidence 자동 생성
    """
    meta = _report_meta(con, report_id)

    # --- (1) biz mode만 evidence_spec으로 판단
    etype = None
    if isinstance(evidence_spec, list):
        evidence_spec = evidence_spec[0] if evidence_spec else None
    if isinstance(evidence_spec, dict):
        etype = (evidence_spec.get("type") or "").strip().lower()

    if etype == "biz":
        topk_biz = int(evidence_spec.get("topk_chunks") or topk_biz_chunks)
        return {
            "corp_code": meta["corp_code"],
            "bsns_year": meta["bsns_year"],
            "report_id": report_id,
            "evidence": _build_biz_evidence(con, report_id, topk_chunks=topk_biz),
        }

    # --- (2) default: notes_by_metrics (metrics_spec 기반 자동)
    metric_keys: List[str] = []
    for r in (metrics_json or {}).get("rows", []) or []:
        mk = r.get("metric_key")
        if mk:
            metric_keys.append(str(mk))

    rows_out = [
        _build_notes_evidence_for_metric(con, report_id, mk, max_notes=max_notes, topk_chunks_per_note=topk_chunks_per_note)
        for mk in metric_keys
    ]

    return {
        "corp_code": meta["corp_code"],
        "bsns_year": meta["bsns_year"],
        "report_id": report_id,
        "evidence": {"type": "notes_by_metrics", "rows": rows_out},
    }


def save_evidence_json(obj: Dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

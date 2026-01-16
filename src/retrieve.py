# src/retrieve.py
# 질문에 필요한 ‘텍스트 설명 + 표 근거’를 모아서 하나의 컨텍스트로 만드는 단계
# 1. 텍스트 검색 (FAISS) : 질문 → embedding  -> FAISS에서 top-k chunk_id 검색  
# -> chunk_id로 DuckDB에서:원문 텍스트, section_code, note_no 등을 가져옴. 
# 2. 주석 표 검색 (DuckDB SQL) : 입력: 사용자 질문 (키워드) -> “주석 12번 표에서 매출채권이 ○○로 구성됨” 같은 정량 근거

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import duckdb

from .utils.normalize import normalize_space

def _escape_like(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

def _build_table_header_paths(con: duckdb.DuckDBPyConnection, table_id: str) -> Dict[int, str]:
    cols = con.execute("""
      SELECT col_idx, coalesce(header_ko,'') AS header_ko
      FROM rag_table_cols
      WHERE table_id = ?
      ORDER BY col_idx
    """, [table_id]).fetchall()

    out = {}
    for col_idx, h in cols:
        hh = normalize_space(h or "")
        if not hh:
            hh = f"COL_{col_idx}"
        out[int(col_idx)] = hh
    return out

def _build_row_label_paths(con: duckdb.DuckDBPyConnection, table_id: str) -> Dict[int, str]:
    rows = con.execute("""
      SELECT row_idx, coalesce(label_ko,'') AS label_ko, parent_row_idx
      FROM rag_table_rows
      WHERE table_id = ?
    """, [table_id]).fetchall()

    by_id = {int(r[0]): {"label": normalize_space(r[1] or ""), "parent": (int(r[2]) if r[2] is not None else None)} for r in rows}

    def path_of(rid: int) -> str:
        seen = set()
        parts = []
        cur = rid
        while cur is not None and cur in by_id and cur not in seen:
            seen.add(cur)
            lab = by_id[cur]["label"] or f"ROW_{cur}"
            parts.append(lab)
            cur = by_id[cur]["parent"]
        parts.reverse()
        return " > ".join(parts)

    return {rid: path_of(rid) for rid in by_id.keys()}

def search_notes_tables(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    query: str,
    topk: int = 20,
    section_id: Optional[str] = None,
    only_statement_type: str = "NOTE",
) -> List[Dict[str, Any]]:
    q = normalize_space(query or "")
    if not q:
        return []

    sql = """
    WITH tbl AS (
      SELECT
        rt.table_id,
        rt.section_id,
        rt.table_title,
        rt.statement_type,
        rs.section_code,
        rs.title_ko,
        rs.note_no
      FROM rag_tables rt
      JOIN report_sections rs
        ON rs.section_id = rt.section_id
      WHERE rs.report_id = ?
        AND rs.section_type = 'notes'
    """
    args = [report_id]

    if section_id:
        sql += " AND rt.section_id = ? "
        args.append(section_id)

    if only_statement_type:
        sql += " AND rt.statement_type = ? "
        args.append(only_statement_type)

    sql += """
    ),
    hits AS (
      SELECT
        c.table_id,
        c.row_idx,
        c.col_idx,
        c.text_value,
        c.num_value,
        t.section_id,
        t.section_code,
        t.title_ko,
        t.note_no,
        t.table_title
      FROM rag_table_cells c
      JOIN tbl t ON t.table_id = c.table_id
      WHERE c.text_value IS NOT NULL
        AND c.text_value != ''
        AND c.text_value ILIKE ? ESCAPE '\\'
    )
    SELECT *
    FROM hits
    LIMIT ?
    """

    pat = f"%{_escape_like(q)}%"
    rows = con.execute(sql, args + [pat, int(topk)]).fetchall()

    out = []
    for (table_id, row_idx, col_idx, text_value, num_value,
         sid, scode, title_ko, note_no, table_title) in rows:
        out.append({
            "table_id": str(table_id),
            "section_id": str(sid),
            "section_code": str(scode),
            "section_title": str(title_ko) if title_ko is not None else "",
            "note_no": int(note_no) if note_no is not None else None,
            "table_title": str(table_title) if table_title is not None else "",
            "row_idx": int(row_idx),
            "col_idx": int(col_idx),
            "text_value": text_value,
            "num_value": float(num_value) if num_value is not None else None,
        })
    return out

def _format_one_row_context(row_idx: int, row_path: str, row_items: List[Tuple[int, str, Optional[float]]], col_headers: Dict[int, str]) -> str:
    kvs = []
    for (col_idx, tv, nv) in row_items:
        h = col_headers.get(int(col_idx), f"COL_{col_idx}")
        tv2 = normalize_space(tv or "")
        if tv2 == "":
            continue
        if nv is not None:
            kvs.append(f"{h}: {tv2} (num={nv:g})")
        else:
            kvs.append(f"{h}: {tv2}")
    if not kvs:
        return f"- row[{row_idx}] {row_path} | (empty)"
    return f"- row[{row_idx}] {row_path} | " + " ; ".join(kvs)

def build_notes_table_context(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    query: str,
    topk_cells: int = 12,
    window_rows: int = 1,
) -> str:
    hits = search_notes_tables(con, report_id=report_id, query=query, topk=topk_cells)
    if not hits:
        return ""

    by_table: Dict[str, List[Dict[str, Any]]] = {}
    for h in hits:
        by_table.setdefault(h["table_id"], []).append(h)

    chunks = []
    for table_id, hs in by_table.items():
        col_headers = _build_table_header_paths(con, table_id)
        row_paths = _build_row_label_paths(con, table_id)

        meta = hs[0]
        header_line = f"[NOTE TABLE] {meta.get('section_code','')} (note_no={meta.get('note_no')}) / {meta.get('section_title','')}\n- table_title: {meta.get('table_title','')}\n- table_id: {table_id}"
        chunks.append(header_line)

        hit_rows = sorted(set(h["row_idx"] for h in hs))
        rows_to_fetch = set()
        for r in hit_rows:
            for rr in range(max(0, r - window_rows), r + window_rows + 1):
                rows_to_fetch.add(rr)
        rows_to_fetch = sorted(rows_to_fetch)

        cells = con.execute("""
          SELECT row_idx, col_idx, coalesce(text_value,'') AS tv, num_value
          FROM rag_table_cells
          WHERE table_id = ?
            AND row_idx IN (SELECT UNNEST(?))
          ORDER BY row_idx, col_idx
        """, [table_id, rows_to_fetch]).fetchall()

        cur_r = None
        row_items = []
        for row_idx, col_idx, tv, nv in cells:
            row_idx = int(row_idx); col_idx = int(col_idx)
            if cur_r is None:
                cur_r = row_idx
            if row_idx != cur_r:
                chunks.append(_format_one_row_context(cur_r, row_paths.get(cur_r, f"ROW_{cur_r}"), row_items, col_headers))
                cur_r = row_idx
                row_items = []
            row_items.append((col_idx, tv, float(nv) if nv is not None else None))
        if cur_r is not None:
            chunks.append(_format_one_row_context(cur_r, row_paths.get(cur_r, f"ROW_{cur_r}"), row_items, col_headers))

        chunks.append("- matched_cells:")
        for h in hs[: min(len(hs), topk_cells)]:
            rp = row_paths.get(h["row_idx"], f"ROW_{h['row_idx']}")
            ch = col_headers.get(h["col_idx"], f"COL_{h['col_idx']}")
            tv = normalize_space(str(h.get("text_value") or ""))
            if len(tv) > 140:
                tv = tv[:140] + "…"
            chunks.append(f"  - ({rp}) / ({ch}) = {tv}")

        chunks.append("")

    return "\n".join(chunks).strip()

def build_context_with_notes_tables(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    user_query: str,
    faiss_text_context: str,
    notes_table_topk_cells: int = 12,
) -> str:
    notes_ctx = build_notes_table_context(
        con=con,
        report_id=report_id,
        query=user_query,
        topk_cells=notes_table_topk_cells,
        window_rows=1,
    )

    parts = []
    if faiss_text_context:
        parts.append("### Retrieved Text (FAISS)\n" + faiss_text_context.strip())
    if notes_ctx:
        parts.append("### Retrieved Notes Tables (SQL)\n" + notes_ctx.strip())

    return "\n\n".join(parts).strip()

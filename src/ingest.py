# src/ingest.py

from __future__ import annotations

import os, re
from typing import List, Dict, Optional, Tuple

import duckdb
import numpy as np
from bs4 import BeautifulSoup

from .utils.ids import stable_id, sha1_hex
from .utils.normalize import (
    NBSP, FULLWIDTH_SPACE,
    normalize_space, split_note_refs, parse_num, normalize_corp_code
)
from .utils.html import (
    strip_html_keep_lines, remove_tables_html,
    get_label_preserve_indent, normalize_label_clean, count_indent
)
from .utils.text import chunk_text, clean_title_ko, detect_statement_type_from_title
from .utils.dart import extract_biz_sections_from_xml, extract_financial_sections_from_xml

# ========== DB schema / migration ==========
def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    r = con.execute("""
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema='main' AND table_name=?
      LIMIT 1
    """, [name]).fetchone()
    return r is not None

def _get_existing_cols(con: duckdb.DuckDBPyConnection, table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]

def init_db(con: duckdb.DuckDBPyConnection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS reports (
      report_id VARCHAR PRIMARY KEY,
      corp_code VARCHAR,
      corp_name VARCHAR,
      bsns_year INTEGER,
      rcept_no VARCHAR,
      report_date DATE,
      source_url VARCHAR
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS report_sections (
      section_id VARCHAR PRIMARY KEY,
      report_id VARCHAR,
      section_code VARCHAR,
      section_type VARCHAR,    -- biz / fs / notes
      note_no INTEGER,
      title_ko VARCHAR,
      title_en VARCHAR,
      sort_order INTEGER,
      raw_html VARCHAR
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS rag_tables (
      table_id VARCHAR PRIMARY KEY,
      section_id VARCHAR,
      statement_type VARCHAR,  -- BS/IS_CIS/CF/NOTE/BIZ/...
      unit_label VARCHAR,
      unit_multiplier BIGINT,
      currency VARCHAR,
      raw_table_html VARCHAR,
      table_title VARCHAR,
      table_order INTEGER
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS rag_table_cols (
      table_id VARCHAR,
      col_idx INTEGER,
      col_type VARCHAR,
      header_ko VARCHAR,
      period_end DATE,
      fiscal_year INTEGER,
      PRIMARY KEY (table_id, col_idx)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS rag_table_rows (
      table_id VARCHAR,
      row_idx INTEGER,
      label_ko VARCHAR,
      label_clean VARCHAR,
      indent_level INTEGER,
      parent_row_idx INTEGER,
      is_abstract BOOLEAN,
      ifrs_code VARCHAR,
      note_refs_raw VARCHAR,
      note_nos INTEGER[],
      PRIMARY KEY (table_id, row_idx)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS rag_table_cells (
      table_id VARCHAR,
      row_idx INTEGER,
      col_idx INTEGER,
      text_value VARCHAR,
      num_value DOUBLE,
      decimals INTEGER,
      acontext VARCHAR,
      PRIMARY KEY (table_id, row_idx, col_idx)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS rag_text_chunks (
      chunk_id VARCHAR PRIMARY KEY,
      report_id VARCHAR,
      section_id VARCHAR,
      section_code VARCHAR,
      section_type VARCHAR,
      note_no INTEGER,
      chunk_idx INTEGER,
      text VARCHAR,
      text_for_embed VARCHAR
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS rag_text_embeddings (
      chunk_id VARCHAR PRIMARY KEY,
      vec_id BIGINT,
      model_name VARCHAR,
      dim INTEGER,
      created_at TIMESTAMP
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS fs_line_items (
      line_item_id VARCHAR PRIMARY KEY,
      statement_type VARCHAR,
      ifrs_code VARCHAR,
      label_ko VARCHAR,
      label_clean VARCHAR
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS fs_facts (
      report_id VARCHAR,
      line_item_id VARCHAR,
      period_end DATE,
      fiscal_year INTEGER,
      value DOUBLE,
      unit_multiplier BIGINT,
      currency VARCHAR,
      table_id VARCHAR,
      row_idx INTEGER,
      col_idx INTEGER,
      note_refs_raw VARCHAR,
      note_nos INTEGER[],
      PRIMARY KEY (report_id, line_item_id, period_end, col_idx)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS note_links (
      report_id VARCHAR,
      line_item_id VARCHAR,
      note_no INTEGER,
      note_section_id VARCHAR,
      confidence DOUBLE,
      PRIMARY KEY (report_id, line_item_id, note_no)
    );
    """)

def ensure_table_schema(con: duckdb.DuckDBPyConnection):
    init_db(con)

    if _table_exists(con, "report_sections"):
        cols = set(_get_existing_cols(con, "report_sections"))
        if "note_no" not in cols:
            con.execute("ALTER TABLE report_sections ADD COLUMN note_no INTEGER")
        if "title_en" not in cols:
            con.execute("ALTER TABLE report_sections ADD COLUMN title_en VARCHAR")

    if _table_exists(con, "rag_tables"):
        cols = set(_get_existing_cols(con, "rag_tables"))
        if "table_title" not in cols:
            con.execute("ALTER TABLE rag_tables ADD COLUMN table_title VARCHAR")
        if "table_order" not in cols:
            con.execute("ALTER TABLE rag_tables ADD COLUMN table_order INTEGER")

    if _table_exists(con, "rag_table_rows"):
        cols = set(_get_existing_cols(con, "rag_table_rows"))
        if "note_nos" not in cols:
            con.execute("ALTER TABLE rag_table_rows ADD COLUMN note_nos INTEGER[]")

    if _table_exists(con, "fs_facts"):
        cols = set(_get_existing_cols(con, "fs_facts"))
        if "note_nos" not in cols:
            con.execute("ALTER TABLE fs_facts ADD COLUMN note_nos INTEGER[]")

    if _table_exists(con, "rag_text_chunks"):
        cols = set(_get_existing_cols(con, "rag_text_chunks"))
        if "text_for_embed" not in cols:
            con.execute("ALTER TABLE rag_text_chunks ADD COLUMN text_for_embed VARCHAR")

# ========== Text chunk upsert ==========
def upsert_text_chunks(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    section_id: str,
    section_code: str,
    section_type: str,
    note_no: Optional[int],
    section_html: str,
    chunk_size: int,
    chunk_overlap: int,
):
    text_only = strip_html_keep_lines(remove_tables_html(section_html))
    if not text_only.strip():
        return

    chunks = chunk_text(text_only, chunk_size, chunk_overlap)
    for idx, c in enumerate(chunks):
        chunk_id = stable_id(report_id, section_id, str(idx), sha1_hex(c))
        con.execute("""
            INSERT OR REPLACE INTO rag_text_chunks
            (chunk_id, report_id, section_id, section_code, section_type, note_no, chunk_idx, text, text_for_embed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (chunk_id, report_id, section_id, section_code, section_type, note_no, idx, c, c))

def upsert_text_chunks_from_text(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    section_id: str,
    section_code: str,
    section_type: str,
    note_no: Optional[int],
    text: str,
    text_for_embed: Optional[str],
    chunk_size: int,
    chunk_overlap: int,
):
    text = (text or "").strip()
    if not text:
        return
    if text_for_embed is None:
        text_for_embed = text

    chunks = chunk_text(text, chunk_size, chunk_overlap)
    embed_chunks = chunk_text(text_for_embed, chunk_size, chunk_overlap)

    m = max(len(chunks), len(embed_chunks))
    while len(chunks) < m: chunks.append("")
    while len(embed_chunks) < m: embed_chunks.append("")

    for idx in range(m):
        c = (chunks[idx] or "").strip()
        e = (embed_chunks[idx] or "").strip()
        if not c:
            continue
        chunk_id = stable_id(report_id, section_id, str(idx), sha1_hex(c))
        con.execute("""
            INSERT OR REPLACE INTO rag_text_chunks
            (chunk_id, report_id, section_id, section_code, section_type, note_no, chunk_idx, text, text_for_embed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (chunk_id, report_id, section_id, section_code, section_type, note_no, idx, c, e))

# ========== Table parsing helpers ==========
_FY_LINE_RE = re.compile(r"제\s*(\d+)\s*기\s*([0-9]{4})\.\s*([0-9]{2})\.\s*([0-9]{2})", re.I)
_UNIT_RE = re.compile(r"\(단위\s*:\s*([^)]+)\)", re.I)

def extract_fy_map(section_html: str) -> Dict[str, Tuple[int, str]]:
    fy_map = {}
    text = strip_html_keep_lines(section_html)
    for m in _FY_LINE_RE.finditer(text):
        gisu = m.group(1)
        y, mm, dd = int(m.group(2)), m.group(3), m.group(4)
        key = f"제{gisu}기"
        fy_map[key.replace(" ", "")] = (y, f"{y}-{mm}-{dd}")
    return fy_map

def extract_unit(section_html: str) -> Tuple[str, int, str]:
    text = strip_html_keep_lines(section_html)
    m = _UNIT_RE.search(text)
    if not m:
        return "", 1, "KRW"
    unit_inner = m.group(1).strip()
    unit_label = unit_inner

    mult = 1
    if "백만" in unit_inner:
        mult = 1_000_000
    elif "천" in unit_inner:
        mult = 1_000
    elif "원" in unit_inner:
        mult = 1
    return unit_label, mult, "KRW"

def attach_parents(rows: List[dict]) -> None:
    stack = []
    for r in rows:
        il = int(r.get("indent_level") or 0)
        while stack and stack[-1][0] >= il:
            stack.pop()
        parent = stack[-1][1] if stack else None
        r["parent_row_idx"] = parent
        stack.append((il, r["row_idx"]))

def rollup_note_nos_to_parents(rows: List[dict]) -> None:
    children = {}
    for r in rows:
        pid = r.get("parent_row_idx")
        if pid is None:
            continue
        children.setdefault(pid, []).append(r["row_idx"])

    by_idx = {r["row_idx"]: r for r in rows}
    visited = set()

    def dfs(idx: int) -> set:
        if idx in visited:
            return set(by_idx[idx].get("note_nos") or [])
        visited.add(idx)

        cur = set(by_idx[idx].get("note_nos") or [])
        for ch in children.get(idx, []):
            cur |= dfs(ch)

        by_idx[idx]["note_nos"] = sorted(cur)
        return cur

    roots = [r["row_idx"] for r in rows if r.get("parent_row_idx") is None]
    for rt in roots:
        dfs(rt)

def parse_fin_table_from_section(section_html: str) -> List[dict]:
    soup = BeautifulSoup(section_html, "lxml")
    tables: List[dict] = []

    for t in soup.find_all("table"):
        border = (t.get("border") or "").strip()
        rules = (t.get("rules") or "").strip().lower()
        if border != "1" and rules != "all":
            continue

        thead = t.find("thead")
        tbody = t.find("tbody")
        if not thead or not tbody:
            continue

        ths = thead.find_all(["th"])
        if len(ths) < 2:
            continue

        col_headers = [normalize_space(th.get_text(" ", strip=True)) for th in ths]

        trs = tbody.find_all("tr")
        if not trs:
            continue

        rows: List[dict] = []
        cells: List[tuple] = []

        row_idx = 0
        for tr in trs:
            tds = tr.find_all(["td", "th", "te"])
            if not tds:
                continue

            first = tds[0]
            label_raw = get_label_preserve_indent(first)
            indent_level = count_indent(label_raw, NBSP, FULLWIDTH_SPACE)

            label_clean0 = normalize_label_clean(label_raw, NBSP, FULLWIDTH_SPACE)
            label_no_note, note_nos, note_raw = split_note_refs(label_clean0)

            prefix = ""
            m = re.match(r"^([\u3000]+)", label_raw or "")
            if m:
                prefix = m.group(1)

            label_ko_clean = prefix + label_no_note

            ifrs_code = None
            if first.name and first.name.lower() == "te":
                ifrs_code = first.get("acode")

            row_nums = []
            for col_idx, td in enumerate(tds[1:], start=1):
                text_val = normalize_space(td.get_text(" ", strip=True))
                dec = td.get("adecimal")
                try:
                    dec_i = int(dec) if dec is not None else None
                except Exception:
                    dec_i = None
                acontext = td.get("acontext")
                num_val = parse_num(text_val)
                cells.append((row_idx, col_idx, text_val if text_val else None, num_val, dec_i, acontext))
                row_nums.append(num_val)

            is_abs = all(v is None for v in row_nums)

            rows.append({
                "row_idx": row_idx,
                "label_ko": label_ko_clean,
                "label_clean": label_no_note,
                "indent_level": indent_level,
                "parent_row_idx": None,
                "ifrs_code": ifrs_code,
                "is_abstract": bool(is_abs),
                "note_refs_raw": f"(주{note_raw})" if note_raw else None,
                "note_nos": note_nos,
            })
            row_idx += 1

        if rows:
            tables.append({
                "col_headers": col_headers,
                "rows": rows,
                "cells": cells,
                "raw_table_html": str(t),
            })

    return tables

def parse_any_table_from_section(section_html: str) -> List[dict]:
    soup = BeautifulSoup(section_html, "lxml")
    out = []

    for t in soup.find_all("table"):
        trs = t.find_all("tr")
        if len(trs) < 2:
            continue

        grid = []
        for tr in trs:
            cells = tr.find_all(["th", "td"])
            row = [normalize_space(c.get_text(" ", strip=True)) for c in cells]
            if row:
                grid.append(row)

        if len(grid) < 2:
            continue

        header = grid[0]
        body = grid[1:]
        col_headers = header if len(header) >= 2 else ["col0", "col1"]

        rows = []
        cells_tuples = []
        for ri, row in enumerate(body):
            if len(row) == 0:
                continue
            label_raw = row[0]
            label_clean, _nums, _raw = split_note_refs(normalize_space(label_raw))

            row_nums = []
            for ci in range(1, max(len(col_headers), len(row))):
                tv = row[ci] if ci < len(row) else ""
                nv = parse_num(tv)
                cells_tuples.append((ri, ci, tv if tv else None, nv, None, None))
                row_nums.append(nv)
            is_abs = all(v is None for v in row_nums)

            rows.append({
                "row_idx": ri,
                "label_ko": label_raw,
                "label_clean": label_clean,
                "indent_level": 0,
                "parent_row_idx": None,
                "ifrs_code": None,
                "is_abstract": bool(is_abs),
                "note_refs_raw": None,
                "note_nos": [],
            })

        if rows:
            out.append({
                "col_headers": col_headers,
                "rows": rows,
                "cells": cells_tuples,
                "raw_table_html": str(t),
            })

    return out

def upsert_tables_common(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    section_id: str,
    statement_type: str,
    section_html: str,
    table_title_prefix: str,
    table_parser: str = "fin",
):
    unit_label, unit_mult, currency = extract_unit(section_html)
    fy_map = extract_fy_map(section_html)

    tables = parse_fin_table_from_section(section_html) if table_parser == "fin" else parse_any_table_from_section(section_html)
    if not tables:
        return

    for ti, t in enumerate(tables):
        table_id = stable_id(section_id, f"table{ti}")
        table_title = f"{table_title_prefix} / table{ti}"

        con.execute("""
          INSERT OR REPLACE INTO rag_tables
          (table_id, section_id, statement_type, unit_label, unit_multiplier, currency, raw_table_html, table_title, table_order)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (table_id, section_id, statement_type, unit_label, unit_mult, currency, t["raw_table_html"], table_title, int(ti)))

        # cols
        col_rows = []
        for col_idx, header in enumerate(t["col_headers"]):
            if col_idx == 0:
                col_rows.append((table_id, col_idx, "line_item", header or "", None, None))
                continue
            key = (header or "").replace(" ", "")
            period_end = None
            fiscal_year = None
            if key in fy_map:
                fiscal_year, period_end = fy_map[key]
            col_rows.append((table_id, col_idx, "period", header or "", period_end, fiscal_year))

        con.executemany(
            "INSERT OR REPLACE INTO rag_table_cols (table_id, col_idx, col_type, header_ko, period_end, fiscal_year) VALUES (?, ?, ?, ?, ?, ?)",
            col_rows
        )

        # rows + parents + rollup
        rows = t["rows"]
        attach_parents(rows)
        rollup_note_nos_to_parents(rows)

        row_rows = []
        for r in rows:
            row_rows.append((
                table_id,
                r["row_idx"],
                r["label_ko"],
                r["label_clean"],
                int(r.get("indent_level") or 0),
                r.get("parent_row_idx"),
                bool(r.get("is_abstract")),
                r.get("ifrs_code"),
                r.get("note_refs_raw"),
                r.get("note_nos") or [],
            ))

        con.executemany(
            """INSERT OR REPLACE INTO rag_table_rows
            (table_id, row_idx, label_ko, label_clean, indent_level, parent_row_idx,
              is_abstract, ifrs_code, note_refs_raw, note_nos)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            row_rows
        )

        # cells
        cell_rows = []
        for (row_idx, col_idx, text_value, num_value, decimals, acontext) in t["cells"]:
            cell_rows.append((table_id, row_idx, col_idx, text_value, num_value, decimals, acontext))
        con.executemany(
            "INSERT OR REPLACE INTO rag_table_cells (table_id, row_idx, col_idx, text_value, num_value, decimals, acontext) VALUES (?, ?, ?, ?, ?, ?, ?)",
            cell_rows
        )

        # FS facts
        if table_parser == "fin":
            col_lookup = {c[1]: (c[4], c[5]) for c in col_rows if c[2] == "period"}
            cell_dict = {(ri, ci): (tv, nv) for (ri, ci, tv, nv, dec, actx) in t["cells"]}

            line_item_rows = []
            facts_rows = []

            for r in rows:
                line_item_id = stable_id(report_id, statement_type, (r.get("ifrs_code") or ""), r["label_clean"])
                line_item_rows.append((line_item_id, statement_type, r.get("ifrs_code"), r["label_ko"], r["label_clean"]))

                rolled_note_nos = r.get("note_nos") or []
                note_refs_raw = r.get("note_refs_raw")

                for col_idx, (period_end, fiscal_year) in col_lookup.items():
                    if period_end is None:
                        continue
                    tv, nv = cell_dict.get((r["row_idx"], col_idx), (None, None))

                    if (tv is None and nv is None) and not rolled_note_nos:
                        continue

                    facts_rows.append((
                        report_id, line_item_id, period_end, fiscal_year, nv,
                        unit_mult, currency, table_id, r["row_idx"], col_idx,
                        note_refs_raw, rolled_note_nos
                    ))

            if line_item_rows:
                con.executemany("""
                  INSERT OR REPLACE INTO fs_line_items
                  (line_item_id, statement_type, ifrs_code, label_ko, label_clean)
                  VALUES (?, ?, ?, ?, ?)
                """, line_item_rows)

            if facts_rows:
                con.executemany("""
                  INSERT OR REPLACE INTO fs_facts
                  (report_id, line_item_id, period_end, fiscal_year, value,
                  unit_multiplier, currency, table_id, row_idx, col_idx,
                  note_refs_raw, note_nos)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, facts_rows)

# ========== Notes robust table + flow token ==========
def _norm_cell_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace(NBSP, " ").replace(FULLWIDTH_SPACE, " ")
    s = s.replace("∼", "~")
    s = normalize_space(s)
    return s

def html_table_to_grid(table_tag) -> List[List[dict]]:
    trs = table_tag.find_all("tr")
    grid: List[List[dict]] = []
    span_map = {}

    for tr in trs:
        row: List[dict] = []
        c = 0

        while c in span_map:
            remain, cell = span_map[c]
            row.append(cell)
            remain -= 1
            if remain <= 0:
                del span_map[c]
            else:
                span_map[c] = (remain, cell)
            c += 1

        for cell_tag in tr.find_all(["th", "td", "te"]):
            while c in span_map:
                remain, cell = span_map[c]
                row.append(cell)
                remain -= 1
                if remain <= 0:
                    del span_map[c]
                else:
                    span_map[c] = (remain, cell)
                c += 1

            text = _norm_cell_text(cell_tag.get_text(" ", strip=True))
            cell = {
                "text": text,
                "is_header": (cell_tag.name.lower() == "th"),
                "attrs": dict(cell_tag.attrs) if hasattr(cell_tag, "attrs") else {},
            }

            rowspan = int(cell_tag.get("rowspan", 1) or 1)
            colspan = int(cell_tag.get("colspan", 1) or 1)

            for k in range(colspan):
                row.append(cell)
                if rowspan > 1:
                    span_map[c + k] = (rowspan - 1, cell)
            c += colspan

        while c in span_map:
            remain, cell = span_map[c]
            row.append(cell)
            remain -= 1
            if remain <= 0:
                del span_map[c]
            else:
                span_map[c] = (remain, cell)
            c += 1

        grid.append(row)

    max_cols = max((len(r) for r in grid), default=0)
    for r in grid:
        while len(r) < max_cols:
            r.append({"text": "", "is_header": False, "attrs": {}})
    return grid

def parse_any_single_table(table_tag) -> Optional[dict]:
    grid = html_table_to_grid(table_tag)
    if len(grid) < 2:
        return None

    first = grid[0]
    header_like = sum(1 for c in first if c.get("is_header"))
    has_header = header_like >= max(1, len(first)//2)

    data_start = 1 if has_header else 0
    col_headers = [_norm_cell_text(c.get("text", "")) for c in (grid[0] if has_header else [{"text": f"COL_{i}"} for i in range(len(first))])]

    rows = []
    cells = []

    out_row_idx = 0
    for r in range(data_start, len(grid)):
        row = grid[r]
        if all(_norm_cell_text(c.get("text","")) == "" for c in row):
            continue

        label_raw = _norm_cell_text(row[0].get("text","")) if row else ""
        label_clean, note_nos, note_raw = split_note_refs(label_raw)

        rows.append({
            "row_idx": out_row_idx,
            "label_ko": label_raw,
            "label_clean": label_clean,
            "indent_level": 0,
            "parent_row_idx": None,
            "ifrs_code": None,
            "is_abstract": False,
            "note_refs_raw": f"(주{note_raw})" if note_raw else None,
            "note_nos": note_nos or [],
        })

        for ci in range(len(col_headers)):
            tv = _norm_cell_text(row[ci].get("text","")) if ci < len(row) else ""
            nv = parse_num(tv)
            cells.append((out_row_idx, ci, tv if tv != "" else None, nv, None, None))

        out_row_idx += 1

    if not rows:
        return None

    return {
        "col_headers": col_headers,
        "rows": rows,
        "cells": cells,
        "raw_table_html": str(table_tag),
    }

_TABLE_TOKEN_RE = re.compile(r"\[\[TABLE:([0-9a-f]{40})\]\]")

def upsert_notes_tables_and_text(
    con: duckdb.DuckDBPyConnection,
    report_id: str,
    section_id: str,
    section_code: str,
    note_no: Optional[int],
    title_ko: str,
    section_html: str,
    chunk_size: int,
    chunk_overlap: int,
):
    soup = BeautifulSoup(section_html, "lxml")
    root = soup.body if soup.body else soup

    parts: List[str] = []
    table_order = 0
    unit_label, unit_mult, currency = extract_unit(section_html)

    for node in root.descendants:
        if getattr(node, "name", None) and node.name.lower() == "table":
            parsed = parse_any_single_table(node)
            if parsed:
                table_id = stable_id(section_id, f"ntable{table_order}")
                table_title = f"{section_code} {title_ko} / ntable{table_order}"

                con.execute("""
                  INSERT OR REPLACE INTO rag_tables
                  (table_id, section_id, statement_type, unit_label, unit_multiplier, currency,
                   raw_table_html, table_title, table_order)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (table_id, section_id, "NOTE", unit_label, unit_mult, currency,
                      parsed["raw_table_html"], table_title, int(table_order)))

                col_rows = []
                for col_idx, header in enumerate(parsed["col_headers"]):
                    col_rows.append((table_id, col_idx, "note_col", header or "", None, None))
                con.executemany(
                    "INSERT OR REPLACE INTO rag_table_cols (table_id, col_idx, col_type, header_ko, period_end, fiscal_year) VALUES (?, ?, ?, ?, ?, ?)",
                    col_rows
                )

                row_rows = []
                for r in parsed["rows"]:
                    row_rows.append((
                        table_id, r["row_idx"], r["label_ko"], r["label_clean"],
                        int(r.get("indent_level") or 0), r.get("parent_row_idx"),
                        bool(r.get("is_abstract")), r.get("ifrs_code"),
                        r.get("note_refs_raw"), r.get("note_nos") or []
                    ))
                con.executemany("""
                    INSERT OR REPLACE INTO rag_table_rows
                    (table_id, row_idx, label_ko, label_clean, indent_level, parent_row_idx,
                     is_abstract, ifrs_code, note_refs_raw, note_nos)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, row_rows)

                cell_rows = []
                for (ri, ci, tv, nv, dec, actx) in parsed["cells"]:
                    cell_rows.append((table_id, ri, ci, tv, nv, dec, actx))
                con.executemany(
                    "INSERT OR REPLACE INTO rag_table_cells (table_id, row_idx, col_idx, text_value, num_value, decimals, acontext) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    cell_rows
                )

                parts.append(f" [[TABLE:{table_id}]] ")
                table_order += 1
            continue

        if isinstance(node, str):
            parent = getattr(node, "parent", None)
            if parent and getattr(parent, "name", None) and parent.name.lower() in ("table", "thead", "tbody", "tr", "td", "th", "te"):
                continue
            txt = _norm_cell_text(node)
            if txt:
                parts.append(txt + " ")

    flow_text = normalize_space("".join(parts)).strip()
    if not flow_text:
        return

    text_for_embed = _TABLE_TOKEN_RE.sub(" (표 포함) ", flow_text)
    text_for_embed = normalize_space(text_for_embed)

    upsert_text_chunks_from_text(
        con=con,
        report_id=report_id,
        section_id=section_id,
        section_code=section_code,
        section_type="notes",
        note_no=note_no,
        text=flow_text,
        text_for_embed=text_for_embed,
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
    )

# ========== Notes sections + note_links ==========
_NOTE_TITLE_NO_RE = re.compile(r"^\s*(\d+)\.\s*", re.I)

def extract_note_no_from_title(title: str) -> Optional[int]:
    if not title:
        return None
    m = _NOTE_TITLE_NO_RE.match(title.strip())
    return int(m.group(1)) if m else None

def save_notes_sections(con, report_id: str, notes_sections: List[Tuple[str,str]]):
    for i, (title, html) in enumerate(notes_sections):
        note_no = extract_note_no_from_title(title)
        title_clean = clean_title_ko(title)

        section_code = f"III-3-{note_no}" if note_no is not None else f"III-3-X{i}"
        section_id = stable_id(report_id, section_code)

        con.execute("""
          INSERT OR REPLACE INTO report_sections
          (section_id, report_id, section_code, section_type, note_no, title_ko, title_en, sort_order, raw_html)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (section_id, report_id, section_code, "notes", note_no, title_clean, None, 3000 + i, html))

def build_note_links(con: duckdb.DuckDBPyConnection, report_id: str):
    note_rows = con.execute("""
      SELECT section_id, note_no
      FROM report_sections
      WHERE report_id = ? AND section_type='notes'
    """, [report_id]).fetchall()

    note_map = {no: sid for (sid, no) in note_rows if no is not None}

    cols = set(_get_existing_cols(con, "fs_facts"))
    if "note_nos" in cols:
        fact_rows = con.execute("""
          SELECT DISTINCT line_item_id, note_nos
          FROM fs_facts
          WHERE report_id = ? AND note_nos IS NOT NULL
        """, [report_id]).fetchall()

        inserts = []
        for line_item_id, note_nos in fact_rows:
            nums = list(note_nos) if note_nos is not None else []
            for no in nums:
                sid = note_map.get(int(no))
                if sid:
                    inserts.append((report_id, line_item_id, int(no), sid, 0.95))
                else:
                    inserts.append((report_id, line_item_id, int(no), None, 0.20))

        if inserts:
            con.executemany(
                "INSERT OR REPLACE INTO note_links (report_id, line_item_id, note_no, note_section_id, confidence) VALUES (?, ?, ?, ?, ?)",
                inserts
            )
        return

    fact_rows = con.execute("""
      SELECT DISTINCT line_item_id, note_refs_raw
      FROM fs_facts
      WHERE report_id = ? AND note_refs_raw IS NOT NULL
    """, [report_id]).fetchall()

    inserts = []
    for line_item_id, note_refs_raw in fact_rows:
        _clean, nums, _raw = split_note_refs(note_refs_raw)
        for no in nums:
            sid = note_map.get(no)
            if sid:
                inserts.append((report_id, line_item_id, no, sid, 0.95))
            else:
                inserts.append((report_id, line_item_id, no, None, 0.20))

    if inserts:
        con.executemany(
            "INSERT OR REPLACE INTO note_links (report_id, line_item_id, note_no, note_section_id, confidence) VALUES (?, ?, ?, ?, ?)",
            inserts
        )

# ========== Meta 조회 helpers (market_data/benchmark_map) ==========
def get_target_meta_from_db(con: duckdb.DuckDBPyConnection, company_name_kr: str, year: int) -> dict:
    rows = con.execute("""
      SELECT
        corp_code,
        corp_name_kr,
        corp_name_en,
        stock_code,
        year,
        asof_date,
        stock_price,
        shares_outstanding,
        scale
      FROM market_data
      WHERE corp_name_kr = ?
        AND year = ?
        AND corp_role = 'target'
      LIMIT 1
    """, [company_name_kr, int(year)]).fetchall()

    if not rows:
        rows = con.execute("""
          SELECT
            corp_code,
            corp_name_kr,
            corp_name_en,
            stock_code,
            year,
            asof_date,
            stock_price,
            shares_outstanding,
            scale
          FROM market_data
          WHERE corp_name_kr = ?
            AND year = ?
          LIMIT 1
        """, [company_name_kr, int(year)]).fetchall()

    if not rows:
        raise ValueError(f"market_data에서 '{company_name_kr}', year={year} 행을 찾지 못했습니다.")

    (corp_code, corp_name_kr, corp_name_en, stock_code, y, asof_date,
     stock_price, shares_outstanding, scale) = rows[0]

    corp_code8 = normalize_corp_code(corp_code)
    return {
        "corp_code": corp_code8,
        "corp_name_kr": str(corp_name_kr),
        "corp_name_en": str(corp_name_en) if corp_name_en is not None else None,
        "stock_code": str(stock_code) if stock_code is not None else None,
        "year": int(y),
        "asof_date": int(asof_date),  # YYYYMMDD
        "stock_price": float(stock_price) if stock_price is not None else None,
        "shares_outstanding": float(shares_outstanding) if shares_outstanding is not None else None,
        "scale": str(scale) if scale is not None else None,
    }

def get_benchmark_company_name_from_db(con: duckdb.DuckDBPyConnection, target_corp_code8: str, year: int) -> str | None:
    r = con.execute("""
      SELECT bench_corp_code, benchmark_name_kr
      FROM benchmark_map
      WHERE corp_code = ?
        AND year = ?
      LIMIT 1
    """, [target_corp_code8, int(year)]).fetchone()

    if not r:
        return None

    bench_code, bench_name = r
    bench_code8 = normalize_corp_code(bench_code)

    if bench_name is not None and str(bench_name).strip():
        return str(bench_name).strip()

    r2 = con.execute("""
      SELECT corp_name_kr
      FROM market_data
      WHERE corp_code = ?
        AND year = ?
        AND corp_role = 'benchmark'
      LIMIT 1
    """, [bench_code8, int(year)]).fetchone()

    if r2 and r2[0]:
        return str(r2[0]).strip()

    r3 = con.execute("""
      SELECT corp_name_kr
      FROM market_data
      WHERE corp_code = ?
        AND year = ?
      LIMIT 1
    """, [bench_code8, int(year)]).fetchone()

    return str(r3[0]).strip() if (r3 and r3[0]) else None

# ========== ingest transaction ==========
def ingest_one_report_xml(
    xml_text: str,
    con: duckdb.DuckDBPyConnection,
    corp_code: str,
    corp_name: str,
    bsns_year: int,
    rcept_no: str,
    chunk_size: int,
    chunk_overlap: int,
) -> str:
    init_db(con)
    ensure_table_schema(con)

    report_id = stable_id(corp_code, str(bsns_year), rcept_no)

    try:
        con.execute("BEGIN TRANSACTION")

        con.execute("""
            INSERT OR REPLACE INTO reports
            (report_id, corp_code, corp_name, bsns_year, rcept_no, report_date, source_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (report_id, corp_code, corp_name, bsns_year, rcept_no, None, None))

        # I/II (biz)
        biz = extract_biz_sections_from_xml(xml_text)

        for i, (title, html) in enumerate(biz.get("I", [])):
            m = re.match(r"^\s*(\d+)\.\s*", title or "")
            n = m.group(1) if m else None
            section_code = f"I-{n}" if n else f"I-X{i}"
            section_id = stable_id(report_id, section_code)

            title_clean = clean_title_ko(title)
            note_no = int(n) if n and n.isdigit() else None

            con.execute("""
            INSERT OR REPLACE INTO report_sections
            (section_id, report_id, section_code, section_type, note_no, title_ko, title_en, sort_order, raw_html)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (section_id, report_id, section_code, "biz", note_no, title_clean, None, 1000 + i, html))

            upsert_text_chunks(con, report_id, section_id, section_code, "biz", note_no, html, chunk_size, chunk_overlap)

        for i, (title, html) in enumerate(biz.get("II", [])):
            m = re.match(r"^\s*(\d+)\.\s*", title or "")
            n = m.group(1) if m else None
            section_code = f"II-{n}" if n else f"II-X{i}"
            section_id = stable_id(report_id, section_code)

            title_clean = clean_title_ko(title)
            note_no = int(n) if n and n.isdigit() else None

            con.execute("""
            INSERT OR REPLACE INTO report_sections
            (section_id, report_id, section_code, section_type, note_no, title_ko, title_en, sort_order, raw_html)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (section_id, report_id, section_code, "biz", note_no, title_clean, None, 1500 + i, html))

            upsert_text_chunks(con, report_id, section_id, section_code, "biz", note_no, html, chunk_size, chunk_overlap)

        # III (fin)
        fin = extract_financial_sections_from_xml(xml_text)
        fs_sections = fin.get("fs", [])
        notes_sections = fin.get("notes", [])

        # III-2 FS tables
        for i, (title, html) in enumerate(fs_sections):
            title_clean = clean_title_ko(title)
            section_code0 = (title.split()[0].replace(".", "") if title else f"X{i}")
            section_code = f"III-2-{section_code0}"
            section_id = stable_id(report_id, section_code)

            con.execute("""
              INSERT OR REPLACE INTO report_sections
              (section_id, report_id, section_code, section_type, note_no, title_ko, title_en, sort_order, raw_html)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (section_id, report_id, section_code, "fs", None, title_clean, None, 2000 + i, html))

            stype = detect_statement_type_from_title(title_clean)
            upsert_tables_common(
                con, report_id, section_id,
                statement_type=stype,
                section_html=html,
                table_title_prefix=f"{section_code} {title_clean}",
                table_parser="fin",
            )

        # III-3 Notes
        save_notes_sections(con, report_id, notes_sections)

        note_rows = con.execute("""
          SELECT section_id, section_code, note_no, title_ko, raw_html
          FROM report_sections
          WHERE report_id=? AND section_type='notes'
          ORDER BY sort_order
        """, [report_id]).fetchall()

        for (sid, scode, note_no, title_ko, raw_html) in note_rows:
            upsert_notes_tables_and_text(
                con=con,
                report_id=report_id,
                section_id=sid,
                section_code=scode,
                note_no=note_no,
                title_ko=title_ko,
                section_html=raw_html,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
            )

        build_note_links(con, report_id)

        con.execute("COMMIT")
        return report_id

    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise

# ========== delete helper ==========
def delete_report(con: duckdb.DuckDBPyConnection, report_id: str):
    if not _table_exists(con, "reports"):
        return

    table_ids = []
    if _table_exists(con, "rag_tables") and _table_exists(con, "report_sections"):
        table_ids = [r[0] for r in con.execute("""
          SELECT rt.table_id
          FROM rag_tables rt
          JOIN report_sections rs ON rs.section_id = rt.section_id
          WHERE rs.report_id = ?
        """, [report_id]).fetchall()]

    if table_ids:
        con.executemany("DELETE FROM rag_table_cells WHERE table_id = ?", [(tid,) for tid in table_ids])
        con.executemany("DELETE FROM rag_table_rows  WHERE table_id = ?", [(tid,) for tid in table_ids])
        con.executemany("DELETE FROM rag_table_cols  WHERE table_id = ?", [(tid,) for tid in table_ids])
        con.executemany("DELETE FROM rag_tables      WHERE table_id = ?", [(tid,) for tid in table_ids])

    chunk_ids = [r[0] for r in con.execute("SELECT chunk_id FROM rag_text_chunks WHERE report_id=?", [report_id]).fetchall()]
    if chunk_ids:
        con.execute("DELETE FROM rag_text_embeddings WHERE chunk_id IN (SELECT UNNEST(?))", [chunk_ids])
        con.execute("DELETE FROM rag_text_chunks     WHERE chunk_id IN (SELECT UNNEST(?))", [chunk_ids])

    con.execute("DELETE FROM fs_facts       WHERE report_id = ?", [report_id])
    con.execute("DELETE FROM note_links     WHERE report_id = ?", [report_id])
    con.execute("DELETE FROM report_sections WHERE report_id = ?", [report_id])
    con.execute("DELETE FROM reports        WHERE report_id = ?", [report_id])

# =============================
# (APPEND) Pipeline helpers from notebook
# =============================
import io
import zipfile
import requests
from datetime import datetime, timedelta
from .utils.dart import (
    extract_biz_sections_from_xml,
    extract_financial_sections_from_xml,
    fetch_document_xml_texts,
    pick_xml_with_iii,
    find_business_report_rcept_no_odr,
)

def ingest_company_year(
    corp_name: str,
    bsns_year: int,
    db_path: str,
    cache_dir: str | None,
    dart_api_key: str,
    window_days: int = 14,
    reprt_code: str = "11011",
    skip_if_exists: bool = True,
) -> str:
    """
    네 cli.py가 import 하는 이름/시그니처에 맞춤:
      ingest_company_year(corp_name, bsns_year, db_path, cache_dir, dart_api_key)

    내부 흐름은 노트북 pipeline 그대로:
      market_data에서 corp_code/asof_date 가져오기
      OpenDartReader로 rcept_no 찾기
      document.xml fetch -> pick_xml_with_iii
      ingest_one_report_xml 실행
    """
    dart_api_key = (dart_api_key or "").strip()
    if not dart_api_key:
        raise RuntimeError("DART_API_KEY가 비어있습니다. .env 또는 --dart-key로 설정하세요.")

    con = duckdb.connect(db_path)
    init_db(con)
    ensure_table_schema(con)

    meta = get_target_meta_from_db(con, corp_name, int(bsns_year))
    corp_code = meta["corp_code"]
    rcept_date = int(meta["asof_date"])

    # OpenDartReader list로 rcept_no 찾기 (노트북 로직 동일)
    import OpenDartReader
    dart = OpenDartReader(dart_api_key)

    rcept_no = find_business_report_rcept_no_odr(
        dart=dart,
        corp_code=corp_code,
        bsns_year=int(bsns_year),
        rcept_date=int(rcept_date),
        window_days=int(window_days),
        reprt_code=reprt_code,
    )

    report_id = stable_id(corp_code, str(int(bsns_year)), str(rcept_no))

    if skip_if_exists:
        exists = con.execute("SELECT 1 FROM reports WHERE report_id=?", [report_id]).fetchone()
        if exists:
            con.close()
            return report_id

    xml_texts = fetch_document_xml_texts(str(rcept_no), dart_api_key)
    xml_text = pick_xml_with_iii(xml_texts)

    report_id2 = ingest_one_report_xml(
        xml_text=xml_text,
        con=con,
        corp_code=corp_code,
        corp_name=corp_name,
        bsns_year=int(bsns_year),
        rcept_no=str(rcept_no),
        chunk_size=1800,
        chunk_overlap=300,
    )

    con.close()
    return report_id2

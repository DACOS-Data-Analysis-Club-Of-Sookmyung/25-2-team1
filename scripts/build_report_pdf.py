# scripts/build_report_pdf.py
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Tuple, Optional
from xml.sax.saxutils import escape

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    PageBreak,
    Table,
    TableStyle,
)
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


# ============================================================
# 0) Evidence label sanitize (internal param -> human label)
#    - IMPORTANT: 절대 줄바꿈을 없애지 않는다. (마크다운 파싱 깨짐 방지)
# ============================================================
# Handles:
# (근거: note_no=1, section_code=자산,
#  chunk_id=현금증가)
EVID_RE = re.compile(
    r"\(근거:\s*note_no\s*=\s*([^,]+?)\s*,\s*section_code\s*=\s*([^,]+?)\s*,\s*chunk_id\s*=\s*([^)]+?)\s*\)",
    re.IGNORECASE | re.DOTALL,
)

def sanitize_evidence_labels(text: str) -> str:
    """
    내부 파라미터명(note_no, section_code, chunk_id)을 PDF에 그대로 노출하지 않도록
    사람이 읽는 표기로 치환한다.
    - 줄바꿈은 유지한다. (마크다운 헤딩/표 파싱에 필수)
    """
    if not text:
        return text

    def repl(m: re.Match) -> str:
        note = m.group(1).strip()
        sec = m.group(2).strip()
        cid = m.group(3).strip()
        # 사람용 표기
        return f"(근거: 주석 {note}, {sec}, 근거ID {cid})"

    return EVID_RE.sub(repl, text)


# ============================================================
# 1) Font + JSON utilities
# ============================================================
def register_korean_font(font_path: Path, font_name: str = "NotoSansKR") -> str:
    if not font_path.exists():
        raise FileNotFoundError(f"Font not found: {font_path}")
    pdfmetrics.registerFont(TTFont(font_name, str(font_path)))
    return font_name


def read_section_json(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def key_section_sort(item: dict) -> tuple:
    """
    section_id가 '1.2', '2.10' 처럼 들어오면 숫자 기준으로 정렬
    """
    sid = str(item.get("section_id", "")).strip()
    parts = []
    for x in sid.split("."):
        try:
            parts.append(int(x))
        except ValueError:
            parts.append(10**9)
    return (parts, str(item.get("id", "")))


# ============================================================
# 2) Minimal Markdown -> Flowables
#    - headings: #, ##, ###
#    - bold: **...**
#    - bullet: - ...
#    - markdown table -> reportlab Table
# ============================================================
BOLD_RE = re.compile(r"\*\*(.+?)\*\*")

def md_inline_to_html(s: str) -> str:
    s = escape(s or "")
    s = BOLD_RE.sub(r"<b>\1</b>", s)
    return s


def _is_md_table_separator(line: str) -> bool:
    # e.g. | --- | --- | or |:---|---:|
    line = line.strip()
    if not (line.startswith("|") and line.endswith("|")):
        return False
    cells = [c.strip() for c in line.strip("|").split("|")]
    if not cells:
        return False
    for c in cells:
        if not c:
            return False
        # only '-' and ':' allowed in separator cell
        c2 = c.replace(":", "").replace("-", "").strip()
        if c2 != "":
            return False
    return True


def _split_md_table_row(line: str) -> List[str]:
    inner = line.strip().strip("|")
    return [c.strip() for c in inner.split("|")]


def parse_md_table(lines: List[str], start_idx: int) -> Tuple[Optional[Table], int]:
    """
    If lines[start_idx:] begins with a markdown table, parse it and return (Table, next_idx).
    Otherwise return (None, start_idx).

    Expected:
      | h1 | h2 |
      | --- | --- |
      | a | b |
    """
    if start_idx >= len(lines):
        return None, start_idx

    header_line = lines[start_idx].rstrip()
    if not (header_line.strip().startswith("|") and header_line.strip().endswith("|")):
        return None, start_idx

    if start_idx + 1 >= len(lines):
        return None, start_idx

    sep_line = lines[start_idx + 1].rstrip()
    if not _is_md_table_separator(sep_line):
        return None, start_idx

    header = _split_md_table_row(header_line)
    rows: List[List[str]] = []
    i = start_idx + 2
    while i < len(lines):
        line = lines[i].rstrip()
        if not (line.strip().startswith("|") and line.strip().endswith("|")):
            break
        rows.append(_split_md_table_row(line))
        i += 1

    ncols = max([len(header)] + [len(r) for r in rows]) if (header or rows) else 0
    if ncols == 0:
        return None, start_idx

    def pad(r: List[str]) -> List[str]:
        return (r + [""] * (ncols - len(r)))[:ncols]

    data = [pad(header)] + [pad(r) for r in rows]
    tbl = Table(data, repeatRows=1)
    return tbl, i


def style_md_table(tbl: Table, font_name: str, font_size: int = 9) -> None:
    """
    Basic Table styling.
    """
    ts = TableStyle(
        [
            ("FONTNAME", (0, 0), (-1, -1), font_name),
            ("FONTSIZE", (0, 0), (-1, -1), font_size),
            ("LEADING", (0, 0), (-1, -1), font_size + 2),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
            ("LINEBELOW", (0, 0), (-1, 0), 1.0, colors.black),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ]
    )
    tbl.setStyle(ts)


def convert_table_cells_to_paragraphs(
    tbl: Table,
    cell_style: ParagraphStyle,
) -> None:
    """
    Replace raw strings in Table with Paragraph to support **bold** etc.
    """
    data = tbl._cellvalues  # noqa
    new_data = []
    for r in data:
        new_row = []
        for c in r:
            if isinstance(c, Paragraph):
                new_row.append(c)
            else:
                new_row.append(Paragraph(md_inline_to_html(str(c)), cell_style))
        new_data.append(new_row)
    tbl._cellvalues = new_data  # noqa


def normalize_markdown_for_pdf(md: str) -> str:
    """
    LLM 출력이 가끔 헤딩 토큰(##/###)이 문장 중간에 붙어 나오는 경우가 있어
    최소한의 정규화만 수행한다.
    - 줄바꿈을 '삭제'하지 않는다.
    """
    if not md:
        return md

    # 근거 표기 먼저 치환 (줄바꿈 유지)
    md = sanitize_evidence_labels(md)

    # 헤딩 토큰이 문장 중간에 붙는 경우: 앞에 줄바꿈을 한 번 넣어 분리
    # (과하게 건드리면 본문이 깨질 수 있어 최소만 적용)
    md = re.sub(r"(?<!\n)(###\s+)", r"\n\1", md)
    md = re.sub(r"(?<!\n)(##\s+)", r"\n\1", md)
    md = re.sub(r"(?<!\n)(#\s+)", r"\n\1", md)

    return md

def remove_lonely_hash_lines(md: str) -> str:
    """
    라인이 '#', '##', '###' 처럼 해시만 덩그러니 있는 경우 제거
    (LLM이 섹션 구분용으로 찍는 쓰레기 라인 방지)
    """
    if not md:
        return md
    lines = md.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    cleaned = []
    for ln in lines:
        if re.fullmatch(r"\s*#{1,6}\s*", ln):
            continue
        cleaned.append(ln)
    return "\n".join(cleaned)

def append_markdown_story(
    story: list,
    md: str,
    styles,
    font_name: str,
) -> None:
    """
    Very small markdown renderer:
    - #, ##, ### headings
    - bullet '- '
    - markdown table -> reportlab Table
    - bold via **...**
    """
    md = normalize_markdown_for_pdf(md or "")
    md = remove_lonely_hash_lines(md)
    lines = md.replace("\r\n", "\n").replace("\r", "\n").splitlines()

    h1 = styles["Heading1"]
    h2 = styles["Heading2"]
    h3 = styles["Heading3"]
    body = styles["BodyText"]

    cell_style = ParagraphStyle(
        "KCell",
        parent=styles["BodyText"],
        fontName=font_name,
        fontSize=9,
        leading=11,
        spaceAfter=0,
        spaceBefore=0,
    )

    i = 0
    while i < len(lines):
        line = lines[i].rstrip()

        if not line.strip():
            story.append(Spacer(1, 4))
            i += 1
            continue

        # Table block
        tbl, next_i = parse_md_table(lines, i)
        if tbl is not None and next_i > i:
            convert_table_cells_to_paragraphs(tbl, cell_style)
            style_md_table(tbl, font_name)
            story.append(Spacer(1, 3))
            story.append(tbl)
            story.append(Spacer(1, 6))
            i = next_i
            continue

        # Headings (must be at line start)
        if line.startswith("### "):
            story.append(Paragraph(md_inline_to_html(line[4:]), h3))
            story.append(Spacer(1, 3))
            i += 1
            continue
        if line.startswith("## "):
            story.append(Paragraph(md_inline_to_html(line[3:]), h2))
            story.append(Spacer(1, 4))
            i += 1
            continue
        if line.startswith("# "):
            story.append(Paragraph(md_inline_to_html(line[2:]), h1))
            story.append(Spacer(1, 6))
            i += 1
            continue

        # Bullets
        if line.startswith("- "):
            story.append(Paragraph("• " + md_inline_to_html(line[2:]), body))
            i += 1
            continue

        # Normal line
        story.append(Paragraph(md_inline_to_html(line), body))
        i += 1




# ============================================================
# 3) PDF builder
# ============================================================
def build_pdf(
    sections_dir: Path,
    out_pdf: Path,
    font_path: Path,
    report_title: str = "자동 생성 리포트",
    show_section_heading_from_json: bool = False,
) -> None:
    """
    sections_dir: 기업/연도 폴더 (여기 안에 *.json 섹션 결과가 있어야 함)
    out_pdf: output pdf path
    font_path: ttf font path
    report_title: 표지 제목
    show_section_heading_from_json:
      - False(추천): LLM content 안에 이미 '## ...' 헤딩이 있을 때 중복 방지
      - True: PDF에서 section_id/title을 별도 헤딩으로 출력
    """
    font_name = register_korean_font(font_path)

    base_styles = getSampleStyleSheet()

    # Korean font styles
    title_style = ParagraphStyle(
        "KTitle",
        parent=base_styles["Title"],
        fontName=font_name,
        leading=28,
        spaceAfter=16,
    )
    h1_style = ParagraphStyle(
        "KH1",
        parent=base_styles["Heading1"],
        fontName=font_name,
        leading=20,
        spaceBefore=6,
        spaceAfter=8,
    )
    h2_style = ParagraphStyle(
        "KH2",
        parent=base_styles["Heading2"],
        fontName=font_name,
        leading=18,
        spaceBefore=6,
        spaceAfter=6,
    )
    h3_style = ParagraphStyle(
        "KH3",
        parent=base_styles["Heading3"],
        fontName=font_name,
        leading=16,
        spaceBefore=4,
        spaceAfter=4,
    )
    body_style = ParagraphStyle(
        "KBody",
        parent=base_styles["BodyText"],
        fontName=font_name,
        leading=14,
        spaceAfter=4,
    )
    meta_style = ParagraphStyle(
        "KMeta",
        parent=base_styles["BodyText"],
        fontName=font_name,
        leading=12,
        textColor=colors.HexColor("#555555"),
        spaceAfter=6,
    )

    styles = {
        "Title": title_style,
        "Heading1": h1_style,
        "Heading2": h2_style,
        "Heading3": h3_style,
        "BodyText": body_style,
    }

    json_paths = sorted(sections_dir.glob("*.json"))
    if not json_paths:
        raise FileNotFoundError(f"No json files found in: {sections_dir}")

    items = [read_section_json(p) for p in json_paths]
    items.sort(key=key_section_sort)

    out_pdf.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=18 * mm,
        bottomMargin=18 * mm,
        title=report_title,
    )

    story: list = []

    # Cover
    story.append(Paragraph(escape(report_title), title_style))
    story.append(Spacer(1, 10))
    story.append(Paragraph(escape(f"총 섹션 수: {len(items)}"), meta_style))
    story.append(PageBreak())

    for idx, it in enumerate(items, start=1):
        section_id = str(it.get("section_id", "")).strip()
        title = str(it.get("title", "")).strip() or f"Section {idx}"
        content = str(it.get("content", "")).strip()

        if show_section_heading_from_json:
            heading = f"{section_id} {title}".strip()
            story.append(Paragraph(escape(heading), h1_style))
            story.append(Spacer(1, 6))

        append_markdown_story(story, content, styles, font_name)
        story.append(PageBreak())

    doc.build(story)


# ============================================================
# 4) CLI usage
# ============================================================
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--sections", type=str, required=True, help="sections json dir (corp/year dir)")
    ap.add_argument("--out", type=str, required=True, help="output pdf path")
    ap.add_argument("--font", type=str, required=True, help="ttf font path")
    ap.add_argument("--title", type=str, default="재무분석 리포트", help="report title")
    ap.add_argument(
        "--show-json-heading",
        action="store_true",
        help="render section_id/title as PDF heading (may duplicate markdown headings)",
    )
    args = ap.parse_args()

    build_pdf(
        sections_dir=Path(args.sections),
        out_pdf=Path(args.out),
        font_path=Path(args.font),
        report_title=args.title,
        show_section_heading_from_json=bool(args.show_json_heading),
    )
    print(f"[OK] saved: {args.out}")

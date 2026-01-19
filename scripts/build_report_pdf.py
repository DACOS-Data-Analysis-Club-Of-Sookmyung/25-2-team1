from __future__ import annotations

import json
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


def register_korean_font(font_path: Path, font_name: str = "NotoSansKR") -> str:
    """
    한글 깨짐 방지: Noto Sans KR 같은 ttf를 등록해서 사용
    """
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
    sid = str(item.get("section_id", ""))
    parts = []
    for x in sid.split("."):
        try:
            parts.append(int(x))
        except ValueError:
            parts.append(10**9)  # 이상한 값은 뒤로
    return (parts, str(item.get("id", "")))


def md_to_paragraph_html(md: str) -> str:
    """
    reportlab Paragraph는 HTML-ish만 받음.
    - 여기서는 최소 변환: 특수문자 escape + 줄바꿈을 <br/>로
    - (굵게/리스트 등 제대로 렌더링하려면 별도 파서가 필요)
    """
    md = md or ""
    md = escape(md)
    md = md.replace("\n", "<br/>")
    return md


def build_pdf(
    sections_dir: Path,
    out_pdf: Path,
    font_path: Path,
    report_title: str = "자동 생성 리포트",
) -> None:
    font_name = register_korean_font(font_path)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "KTitle",
        parent=styles["Title"],
        fontName=font_name,
        leading=28,
        spaceAfter=18,
    )
    h1_style = ParagraphStyle(
        "KH1",
        parent=styles["Heading1"],
        fontName=font_name,
        leading=20,
        spaceBefore=6,
        spaceAfter=10,
    )
    body_style = ParagraphStyle(
        "KBody",
        parent=styles["BodyText"],
        fontName=font_name,
        leading=15,
        spaceAfter=8,
    )
    meta_style = ParagraphStyle(
        "KMeta",
        parent=styles["BodyText"],
        fontName=font_name,
        leading=13,
        textColor="#555555",
        spaceAfter=6,
    )

    # 섹션 JSON 모으기
    json_paths = sorted(sections_dir.glob("*.json"))
    if not json_paths:
        raise FileNotFoundError(f"No json files found in: {sections_dir}")

    items = [read_section_json(p) for p in json_paths]
    items.sort(key=key_section_sort)

    doc = SimpleDocTemplate(
        str(out_pdf),
        pagesize=A4,
        leftMargin=40,
        rightMargin=40,
        topMargin=40,
        bottomMargin=40,
        title=report_title,
    )

    story = []
    # 표지
    story.append(Paragraph(escape(report_title), title_style))
    story.append(Spacer(1, 12))
    story.append(Paragraph(escape(f"총 섹션 수: {len(items)}"), meta_style))
    story.append(PageBreak())

    # 본문
    for idx, it in enumerate(items, start=1):
        section_id = str(it.get("section_id", "")).strip()
        title = str(it.get("title", "")).strip() or f"Section {idx}"
        content = str(it.get("content", "")).strip()

        heading = f"{section_id} {title}".strip()
        story.append(Paragraph(escape(heading), h1_style))
        story.append(Spacer(1, 6))

        story.append(Paragraph(md_to_paragraph_html(content), body_style))
        story.append(PageBreak())

    doc.build(story)


if __name__ == "__main__":
    SECTIONS_DIR = Path("outputs/sections")          # 섹션 json 있는 폴더
    OUT_PDF = Path("outputs/report.pdf")            # 출력 pdf 경로
    FONT_PATH = Path("/usr/share/fonts/truetype/nanum/NanumGothic.ttf")  # 한글 폰트 경로

    OUT_PDF.parent.mkdir(parents=True, exist_ok=True)

    build_pdf(
        sections_dir=SECTIONS_DIR,
        out_pdf=OUT_PDF,
        font_path=FONT_PATH,
        report_title="재무분석 리포트",
    )
    print(f"[OK] saved: {OUT_PDF}")

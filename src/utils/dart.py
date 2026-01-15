# src/utils/dart.py
import io, re, zipfile
import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple
from datetime import datetime, timedelta

import pandas as pd
import requests

from .normalize import normalize_space

def ensure_str(x):
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8")
        except UnicodeDecodeError:
            return x.decode("cp949", errors="ignore")
    return x

def fetch_document_xml_texts(rcept_no: str, api_key: str) -> List[str]:
    url = "https://opendart.fss.or.kr/api/document.xml"
    params = {"crtfc_key": api_key, "rcept_no": rcept_no}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    content = r.content

    if content[:2] == b"PK":
        zf = zipfile.ZipFile(io.BytesIO(content))
        xml_text_list = []
        for info in sorted(zf.infolist(), key=lambda x: x.filename):
            data = zf.read(info.filename)
            xml_text_list.append(ensure_str(data))
        return xml_text_list

    try:
        tree = ET.fromstring(content)
        raise RuntimeError(f"document.xml 오류: {tree.findtext('status')} {tree.findtext('message')}")
    except ET.ParseError:
        raise RuntimeError("document.xml 응답이 zip도 xml도 아닙니다.")

def pick_xml_with_iii(xml_texts: List[str]) -> str:
    for x in xml_texts:
        if re.search(r"<TITLE[^>]*>\s*III\.\s*재무에\s*관한\s*사항\s*</TITLE>", x, flags=re.I):
            return x
    return max(xml_texts, key=lambda s: len(s)) if xml_texts else ""

def find_block(text: str, start_pat: str, end_pat_list: List[str]) -> str:
    m_start = re.search(start_pat, text, flags=re.I | re.S)
    if not m_start:
        raise ValueError(f"시작 패턴을 찾지 못했습니다: {start_pat}")
    start = m_start.start()

    end = len(text)
    sub = text[m_start.end():]
    for pat in end_pat_list:
        m_end = re.search(pat, sub, flags=re.I | re.S)
        if m_end:
            end = min(end, m_start.end() + m_end.start())
    return text[start:end].strip()

def split_sections_by_titles(block_html: str, title_filter_re: re.Pattern) -> List[Tuple[str, str]]:
    matches = []
    for m in re.finditer(r"<TITLE[^>]*>\s*(.*?)\s*</TITLE>", block_html, flags=re.I | re.S):
        title = normalize_space(m.group(1))
        if title_filter_re.search(title):
            matches.append((m.start(), m.end(), title))

    if not matches:
        return []

    sections = []
    for i, (st, ed, title) in enumerate(matches):
        nxt = matches[i+1][0] if i+1 < len(matches) else len(block_html)
        sec_html = block_html[st:nxt]
        sections.append((title, sec_html))
    return sections

def extract_biz_sections_from_xml(xml_text: str) -> Dict[str, List[Tuple[str,str]]]:
    text = ensure_str(xml_text)

    i_block = find_block(
        text,
        start_pat=r"<TITLE[^>]*>\s*I\.\s*회사의\s*개요\s*</TITLE>",
        end_pat_list=[r"<TITLE[^>]*>\s*II\.", r"<TITLE[^>]*>\s*II\s*\."]  # 느슨하게
    )
    ii_block = find_block(
        text,
        start_pat=r"<TITLE[^>]*>\s*II\.\s*사업의\s*내용\s*</TITLE>",
        end_pat_list=[r"<TITLE[^>]*>\s*III\.", r"<TITLE[^>]*>\s*III\s*\."]  # 느슨하게
    )

    i_sections = split_sections_by_titles(i_block,  re.compile(r"^\d+\.\s+", re.I))
    ii_sections = split_sections_by_titles(ii_block, re.compile(r"^\d+\.\s+", re.I))

    return {"I": i_sections, "II": ii_sections}

def extract_financial_sections_from_xml(xml_text: str) -> Dict[str, List[Tuple[str,str]]]:
    text = ensure_str(xml_text)

    iii_block = find_block(
        text,
        start_pat=r"<TITLE[^>]*>\s*III\.\s*재무에\s*관한\s*사항\s*</TITLE>",
        end_pat_list=[r"<TITLE[^>]*>\s*IV\.", r"<TITLE[^>]*>\s*IV\s*\."]  # 느슨하게
    )

    fs_block = find_block(
        iii_block,
        start_pat=r"<TITLE[^>]*>\s*2\.\s*연결재무제표\s*</TITLE>",
        end_pat_list=[r"<TITLE[^>]*>\s*3\.\s*연결재무제표\s*주석\s*</TITLE>", r"<TITLE[^>]*>\s*IV\."]  # 느슨하게
    )

    notes_block = find_block(
        iii_block,
        start_pat=r"<TITLE[^>]*>\s*3\.\s*연결재무제표\s*주석\s*</TITLE>",
        end_pat_list=[r"<TITLE[^>]*>\s*4\.\s*재무제표\s*</TITLE>", r"<TITLE[^>]*>\s*IV\."]  # 느슨하게
    )

    fs_sections = split_sections_by_titles(
        fs_block,
        title_filter_re=re.compile(r"^2-\d+\.\s*", re.I)
    )

    notes_sections = split_sections_by_titles(
        notes_block,
        title_filter_re=re.compile(r"^\d+\.\s+.*\(\s*연결\s*\)\s*$", re.I)
    )

    return {"fs": fs_sections, "notes": notes_sections}

# ---- OpenDartReader 호환 + rcept_no 찾기 ----
def odr_list_compat(dart, corp_code: str, bgn_de: str, end_de: str) -> pd.DataFrame:
    try:
        df = dart.list(corp_code, bgn_de, end_de)
        return df if df is not None else pd.DataFrame()
    except TypeError:
        pass
    try:
        df = dart.list(corp=corp_code, bgn_de=bgn_de, end_de=end_de)
        return df if df is not None else pd.DataFrame()
    except TypeError:
        pass
    try:
        df = dart.list(corp_code=corp_code, bgn_de=bgn_de, end_de=end_de)
        return df if df is not None else pd.DataFrame()
    except TypeError as e:
        raise TypeError(f"OpenDartReader.list() 호출 실패: {e} / corp_code={corp_code}, {bgn_de}~{end_de}")

def find_business_report_rcept_no_odr(
    dart,
    corp_code: str,
    bsns_year: int,
    rcept_date: int,
    window_days: int,
    reprt_code: str,
) -> str:
    d0 = datetime.strptime(str(rcept_date), "%Y%m%d")
    bgn = (d0 - timedelta(days=window_days)).strftime("%Y%m%d")
    end = (d0 + timedelta(days=window_days)).strftime("%Y%m%d")

    df = odr_list_compat(dart, corp_code=corp_code, bgn_de=bgn, end_de=end)
    if df is None or len(df) == 0:
        raise RuntimeError(f"dart.list 결과 비어있음: corp={corp_code}, {bgn}~{end}")

    for c in ["rcept_no", "report_nm"]:
        if c not in df.columns:
            raise RuntimeError(f"dart.list 결과에 '{c}' 없음. columns={list(df.columns)}")

    cand = df[df["report_nm"].astype(str).str.contains("사업보고서", na=False)].copy()

    if "bsns_year" in cand.columns:
        cand = cand[cand["bsns_year"].astype(str) == str(bsns_year)]
    if len(cand) == 0:
        cand = df[df["report_nm"].astype(str).str.contains("사업보고서", na=False)].copy()

    if len(cand) == 0:
        raise RuntimeError(f"사업보고서 후보가 없음: corp={corp_code}, {bgn}~{end}")

    if "rcept_dt" in cand.columns:
        cand["rcept_dt_int"] = cand["rcept_dt"].astype(str).str.replace("-", "")
        cand["rcept_dt_int"] = pd.to_numeric(cand["rcept_dt_int"], errors="coerce")
        cand["dist"] = (cand["rcept_dt_int"] - int(rcept_date)).abs()
        cand = cand.sort_values(["dist"], ascending=True)
    else:
        cand = cand.sort_values(["rcept_no"], ascending=False)

    return str(cand.iloc[0]["rcept_no"])

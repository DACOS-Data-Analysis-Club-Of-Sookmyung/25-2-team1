# src/utils/text.py
import re
from typing import List

from .normalize import normalize_space

def chunk_text(text: str, chunk_size: int, overlap: int) -> List[str]:
    """
    간단 청킹: 문단/문장 경계 힌트로 자르고 overlap 적용.
    """
    text = (text or "").strip()
    if not text:
        return []
    chunks = []
    i = 0
    n = len(text)
    while i < n:
        j = min(n, i + chunk_size)
        window = text[i:j]
        cut = max(window.rfind("\n\n"), window.rfind("\n"), window.rfind(". "), window.rfind("다. "))
        if cut > int(chunk_size * 0.6):
            j = i + cut + 1
        chunks.append(text[i:j].strip())
        if j >= n:
            break
        i = max(0, j - overlap)
    return [c for c in chunks if c]

_TITLE_PREFIX_RE = re.compile(r"^\s*(?:\d+(?:-\d+)*)(?:\s*[\.\)]\s*|\s+)", re.UNICODE)

def clean_title_ko(title: str) -> str:
    title = normalize_space(title or "")
    title = _TITLE_PREFIX_RE.sub("", title).strip()
    return title

def _normalize_title_for_match(title: str) -> str:
    t = normalize_space(title or "")
    t = t.replace(" ", "")
    t = t.replace("-", "")
    t = t.replace("‐", "").replace("–", "").replace("—", "")
    t = t.replace("(", "").replace(")", "")
    return t

def detect_statement_type_from_title(title: str) -> str:
    """
    FS 섹션 title(예: '연결 재무상태표', '연결 포괄손익계산서' 등)에서
    statement_type을 BS / IS_CIS / CE / CF 로 매핑.
    """
    if not title:
        return "FS"

    t = _normalize_title_for_match(title)

    if "현금흐름표" in t:
        return "CF"
    if ("자본변동표" in t) or ("자본변동" in t):
        return "CE"
    if ("포괄손익계산서" in t) or ("포괄손익" in t) or ("손익계산서" in t):
        return "IS_CIS"
    if ("재무상태표" in t) or ("대차대조표" in t):
        return "BS"

    return "FS"

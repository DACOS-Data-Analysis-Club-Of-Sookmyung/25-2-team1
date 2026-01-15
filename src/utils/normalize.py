# src/utils/normalize.py
import re
from typing import Optional, Tuple, List

import numpy as np
import pandas as pd

FULLWIDTH_SPACE = "\u3000"
NBSP = "\u00A0"

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def normalize_corp_code(x) -> str:
    if pd.isna(x):
        return ""
    s = str(int(x)) if isinstance(x, (int, np.integer, float, np.floating)) else str(x)
    s = re.sub(r"\D", "", s)
    return s.zfill(8)

_NOTE_RE = re.compile(r"\(\s*주\s*([0-9,\s]+)\)", re.I)

def split_note_refs(s: str) -> Tuple[str, List[int], str]:
    """
    '(주4,28)' 형태를 찾아:
      - clean label
      - note_nos [4, 28]
      - raw "4,28"
    반환
    """
    if not s:
        return "", [], ""
    m = _NOTE_RE.search(s)
    if not m:
        return s.strip(), [], ""
    raw = m.group(1)
    nums = []
    for tok in re.split(r"[,\s]+", raw.strip()):
        if tok.isdigit():
            nums.append(int(tok))
    clean = _NOTE_RE.sub("", s).strip()
    return clean, nums, raw.strip()

def parse_num(text: str) -> Optional[float]:
    """
    숫자 파싱: '(1,234)' -> -1234.0, '-'/'—' 등은 None
    """
    if text is None:
        return None
    t = normalize_space(str(text))
    if not t or t in ("-", "—", "–"):
        return None
    neg = False
    if t.startswith("(") and t.endswith(")"):
        neg = True
        t = t[1:-1]
    t = t.replace(",", "")
    if not re.match(r"^\-?\d+(\.\d+)?$", t):
        return None
    v = float(t)
    return -v if neg else v

# src/utils/ids.py

import hashlib
from typing import Optional

def sha1_hex(s: str) -> str:
    return hashlib.sha1((s or "").encode("utf-8", errors="ignore")).hexdigest()

def stable_id(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update((p or "").encode("utf-8", errors="ignore"))
        h.update(b"\x1f")
    return h.hexdigest()

def chunk_id_to_int64(chunk_id_hex40: str) -> int:
    """
    FAISS add_with_ids에 넣기 위해 40hex chunk_id를 int64로 변환(충돌 확률 매우 낮음).
    """
    x = int(chunk_id_hex40[-16:], 16) # 뒤 16 hex 사용
    if x >= (1 << 63):
        x -= (1 << 64)
    return x

# src/embed.py
from __future__ import annotations

import os
from typing import Optional

import duckdb
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer


def _get_existing_cols(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return [r[1] for r in rows]


def chunk_id_to_int64(chunk_id_hex40: str) -> int:
    # 노트북에서 쓰던 방식과 동일(앞 16hex를 int64로)
    return int(chunk_id_hex40[:16], 16) - (1 << 63)


def load_or_create_faiss(index_path: str, dim: int) -> faiss.Index:
    if os.path.exists(index_path):
        index = faiss.read_index(index_path)
        if index.d != dim:
            raise ValueError(f"FAISS dim mismatch: index.d={index.d}, model dim={dim}")
        return index
    base = faiss.IndexFlatIP(dim)
    return faiss.IndexIDMap2(base)


def normalize_embeddings(x: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(x, axis=1, keepdims=True) + 1e-12
    return x / norms


# ============================================================
# ✅ pipeline.py가 기대하는 함수 (con을 받아서 같은 커넥션으로 처리)
# ============================================================
def build_or_update_faiss_from_db(
    con: duckdb.DuckDBPyConnection,
    index_path: str,
    model_name: str,
    batch_size: int = 128,
    rebuild: bool = False,
) -> None:
    # text 컬럼 선택
    text_col = "text_for_embed" if "text_for_embed" in _get_existing_cols(con, "rag_text_chunks") else "text"

    model = SentenceTransformer(model_name)
    dim = model.get_sentence_embedding_dimension()

    # embeddings 메타 테이블 보장
    con.execute("""
      CREATE TABLE IF NOT EXISTS rag_text_embeddings (
        chunk_id VARCHAR PRIMARY KEY,
        vec_id BIGINT,
        model_name VARCHAR,
        dim INTEGER,
        created_at TIMESTAMP
      )
    """)

    if rebuild:
        # 새로 만들기
        base = faiss.IndexFlatIP(dim)
        index = faiss.IndexIDMap2(base)

        con.execute("DELETE FROM rag_text_embeddings WHERE model_name = ?", [model_name])

        rows = con.execute(f"""
          SELECT chunk_id, {text_col}
          FROM rag_text_chunks
          ORDER BY report_id, section_code, chunk_idx
        """).fetchall()

        if not rows:
            faiss.write_index(index, index_path)
            print("✅ REBUILD: rag_text_chunks 비어있음 -> 빈 FAISS 저장")
            return

        now_ts = con.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]
        print(f"🔁 REBUILD FAISS: {len(rows)} chunks (col={text_col})")

        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            chunk_ids = [cid for (cid, _t) in batch]
            texts = [t if t is not None else "" for (_cid, t) in batch]

            emb = model.encode(
                texts,
                batch_size=min(len(texts), 64),
                show_progress_bar=False,
                convert_to_numpy=True
            ).astype("float32")
            emb = normalize_embeddings(emb)

            vec_ids = np.array([chunk_id_to_int64(cid) for cid in chunk_ids], dtype=np.int64)
            index.add_with_ids(emb, vec_ids)

            for cid, vid in zip(chunk_ids, vec_ids.tolist()):
                con.execute("""
                    INSERT OR REPLACE INTO rag_text_embeddings
                    (chunk_id, vec_id, model_name, dim, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (cid, int(vid), model_name, int(dim), now_ts))

        faiss.write_index(index, index_path)
        print(f"✅ FAISS rebuilt: {index_path} ntotal={index.ntotal}")
        return

    # --- update mode ---
    index = load_or_create_faiss(index_path, dim)

    rows = con.execute(f"""
      SELECT chunk_id, {text_col}
      FROM rag_text_chunks
      WHERE chunk_id NOT IN (
        SELECT chunk_id
        FROM rag_text_embeddings
        WHERE model_name = ?
      )
      ORDER BY report_id, section_code, chunk_idx
    """, [model_name]).fetchall()

    if not rows:
        faiss.write_index(index, index_path)
        print("✅ UPDATE: new chunks 없음")
        return

    now_ts = con.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]
    print(f"🔎 new chunks: {len(rows)} (col={text_col})")

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        chunk_ids = [cid for (cid, _t) in batch]
        texts = [t if t is not None else "" for (_cid, t) in batch]

        emb = model.encode(
            texts,
            batch_size=min(len(texts), 64),
            show_progress_bar=False,
            convert_to_numpy=True
        ).astype("float32")
        emb = normalize_embeddings(emb)

        vec_ids = np.array([chunk_id_to_int64(cid) for cid in chunk_ids], dtype=np.int64)

        # 같은 id가 이미 있으면 제거 후 add (안정)
        try:
            index.remove_ids(vec_ids)
        except Exception:
            pass

        index.add_with_ids(emb, vec_ids)

        for cid, vid in zip(chunk_ids, vec_ids.tolist()):
            con.execute("""
                INSERT OR REPLACE INTO rag_text_embeddings
                (chunk_id, vec_id, model_name, dim, created_at)
                VALUES (?, ?, ?, ?, ?)
            """, (cid, int(vid), model_name, int(dim), now_ts))

    faiss.write_index(index, index_path)
    print(f"✅ FAISS updated: {index_path} ntotal={index.ntotal}")


# ============================================================
# ✅ CLI가 쓰는 함수 (db_path 받아서 connect해서 처리)
#    -> 내부적으로 위 wrapper를 재사용하도록 정리
# ============================================================
def embed_build_or_update(
    db_path: str,
    index_path: str,
    model_name: str,
    batch_size: int = 128,
    rebuild: bool = False,
) -> None:
    con = duckdb.connect(db_path)
    try:
        build_or_update_faiss_from_db(
            con=con,
            index_path=index_path,
            model_name=model_name,
            batch_size=batch_size,
            rebuild=rebuild,
        )
    finally:
        con.close()

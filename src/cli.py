# src/cli.py
"""
src/cli.py
- 공식 커맨드 엔트리포인트 (Windows/리눅스 공통)

  # 0) (최초 1회 권장) market seed
  python -m src.cli seed-market

  # 1) (선택) market validate
  python -m src.cli validate-market

  # 2) ingest + embed (최종 사용자 UX: 기업명 + 연도만)
  python -m src.cli run --company "삼성전자" --year 2024 --run-benchmark --skip-if-exists

  # 3) 개별 실행(개발/디버깅용)
  python -m src.cli ingest --company "삼성전자" --year 2024
  python -m src.cli embed
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Optional, Any, Dict

import duckdb
from dotenv import load_dotenv

# ✅ .env 로드 (DART_API_KEY, DB_PATH, FAISS_INDEX_PATH, CSV_PATH, EMBED_MODEL_NAME ...)
load_dotenv()


# -----------------------------
# Paths / env helpers
# -----------------------------
def project_root() -> Path:
    # repo root = cli.py 기준 1단계 상위 (src/cli.py)
    return Path(__file__).resolve().parents[1]


def env_default(name: str, fallback: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(name)
    if v is None:
        return fallback
    v = str(v).strip()
    return v if v else fallback


def _resolve_path_from_env(root: Path, env_name: str, fallback: Path) -> Path:
    """
    - env 값이 절대경로면 그대로
    - env 값이 상대경로면 project root 기준으로 해석
    """
    v = env_default(env_name)
    if v:
        p = Path(v)
        return p if p.is_absolute() else (root / p)
    return fallback


def default_db_path(root: Path) -> Path:
    return _resolve_path_from_env(root, "DB_PATH", root / "data" / "duckdb" / "dart.duckdb")


def default_faiss_index_path(root: Path) -> Path:
    return _resolve_path_from_env(root, "FAISS_INDEX_PATH", root / "data" / "faiss" / "index.faiss")


def default_faiss_meta_path(root: Path) -> Path:
    return _resolve_path_from_env(root, "FAISS_META_PATH", root / "data" / "faiss" / "meta.json")


def default_csv_path(root: Path) -> Path:
    return _resolve_path_from_env(root, "CSV_PATH", root / "data" / "company_meta.csv")


def default_embed_model() -> str:
    return env_default("EMBED_MODEL_NAME", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")  # type: ignore


def require_dart_key(arg_val: Optional[str] = None) -> str:
    key = (arg_val or env_default("DART_API_KEY") or "").strip()
    if not key:
        raise RuntimeError(
            "DART API KEY가 없습니다.\n"
            "- 방법1) 환경변수: set DART_API_KEY=... (Windows) / export DART_API_KEY=... (Linux/Mac)\n"
            "- 방법2) .env에 DART_API_KEY=... 추가\n"
            "- 방법3) --dart-key 옵션으로 전달"
        )
    return key


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


# -----------------------------
# CLI
# -----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dart-financial-genai-report")
    sub = p.add_subparsers(dest="cmd", required=True)

    # --- seed-market ---
    p_seed = sub.add_parser("seed-market", help="CSV -> market_data / benchmark_map")
    p_seed.add_argument("--db", default=None, help="DuckDB 경로(기본: env DB_PATH 또는 data/duckdb/dart.duckdb)")
    p_seed.add_argument("--csv", default=None, help="CSV 경로(기본: env CSV_PATH 또는 data/company_meta.csv)")
    p_seed.add_argument("--overwrite", action="store_true", help="기존 테이블 덮어쓰기")

    # --- validate-market ---
    sub.add_parser("validate-market", help="validate market tables (market_data/benchmark_map)")

    # --- ingest ---
    p_ingest = sub.add_parser("ingest", help="crawl + normalize + store to DuckDB")
    # ✅ UX 통일: --company
    p_ingest.add_argument("--company", required=True, help="기업명(예: 삼성전자)")
    p_ingest.add_argument("--year", required=True, type=int, help="사업연도(예: 2024)")
    p_ingest.add_argument("--db", default=None, help="DuckDB 경로(기본: env DB_PATH 또는 data/duckdb/dart.duckdb)")
    p_ingest.add_argument("--cache", default=None, help="캐시 디렉토리(기본: data/cache)")
    p_ingest.add_argument("--dart-key", default=None, help="DART API KEY (기본: env DART_API_KEY)")

    # --- embed ---
    p_embed = sub.add_parser("embed", help="build/update FAISS from rag_text_chunks")
    p_embed.add_argument("--db", default=None, help="DuckDB 경로(기본: env DB_PATH 또는 data/duckdb/dart.duckdb)")
    p_embed.add_argument("--index", default=None, help="FAISS index 경로(기본: env FAISS_INDEX_PATH 또는 data/faiss/index.faiss)")
    p_embed.add_argument("--model", default=None, help="임베딩 모델(기본: env EMBED_MODEL_NAME)")
    p_embed.add_argument("--rebuild", action="store_true", help="기존 인덱스 무시하고 전체 재빌드")

    # --- run (최종 UX) ---
    p_run = sub.add_parser("run", help="ingest + embed (+ optional benchmark)")
    p_run.add_argument("--company", required=True, help="기업명(예: 삼성전자)")
    p_run.add_argument("--year", required=True, type=int, help="사업연도(예: 2024)")
    p_run.add_argument("--run-benchmark", action="store_true", help="벤치마크 기업도 같이 ingest")
    p_run.add_argument("--skip-if-exists", action="store_true", help="이미 있으면 ingest 스킵")

    return p


def _jsonable_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    validate-market에서 pandas.DataFrame이 dict에 들어있어서 json.dumps가 터지는 문제 해결
    """
    try:
        import pandas as pd  # type: ignore
    except Exception:
        pd = None  # type: ignore

    out: Dict[str, Any] = {}
    for k, v in d.items():
        if pd is not None and hasattr(pd, "DataFrame") and isinstance(v, pd.DataFrame):
            out[k] = v.to_dict(orient="records")
        else:
            out[k] = v
    return out


def main(argv=None) -> int:
    root = project_root()
    args = build_parser().parse_args(argv)

    # env defaults (경로는 default_*에서 root 기준으로 처리)
    env_model = (env_default("EMBED_MODEL_NAME", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2") or "").strip()

    if args.cmd == "seed-market":
        db_path = Path(args.db) if args.db else default_db_path(root)
        csv_path = Path(args.csv) if args.csv else default_csv_path(root)
        ensure_parent_dir(db_path)

        from .seed_market import seed_market_from_csv
        seed_market_from_csv(
            db_path=str(db_path),
            csv_path=str(csv_path),
            overwrite=bool(args.overwrite),
        )
        print("✅ seed-market done.")
        return 0

    if args.cmd == "validate-market":
        from .validate import validate_market_tables
        db_path = default_db_path(root)
        ensure_parent_dir(db_path)

        con = duckdb.connect(str(db_path))
        out = validate_market_tables(con)
        con.close()

        out2 = _jsonable_dict(out)
        print(json.dumps(out2, indent=2, ensure_ascii=False))
        return 0

    if args.cmd == "ingest":
        dart_key = require_dart_key(args.dart_key)

        db_path = Path(args.db) if args.db else default_db_path(root)
        cache_dir = Path(args.cache) if args.cache else (root / "data" / "cache")
        ensure_parent_dir(db_path)
        ensure_dir(cache_dir)

        from .ingest import ingest_company_year
        report_id = ingest_company_year(
            corp_name=args.company,
            bsns_year=args.year,
            db_path=str(db_path),
            cache_dir=str(cache_dir),
            dart_api_key=dart_key,
        )
        print("✅ ingest done. report_id =", report_id)
        return 0

    if args.cmd == "embed":
        db_path = Path(args.db) if args.db else default_db_path(root)
        index_path = Path(args.index) if args.index else default_faiss_index_path(root)
        model_name = (args.model or env_model or default_embed_model()).strip()

        ensure_parent_dir(db_path)
        ensure_parent_dir(index_path)

        from .embed import embed_build_or_update
        embed_build_or_update(
            db_path=str(db_path),
            index_path=str(index_path),
            model_name=model_name,
            rebuild=bool(args.rebuild),
        )
        print("✅ embed done.")
        return 0

    if args.cmd == "run":
        from .pipeline import PipelineConfig, run_pipeline_for_company_year

        cfg = PipelineConfig(
            api_key=require_dart_key(),
            embed_model_name=default_embed_model(),
            reprt_code_biz="11011",
            chunk_size=1800,
            chunk_overlap=300,
        )

        # run은 기본 경로를 사용 (원하면 여기에도 --db/--index 옵션 추가 가능)
        db_path = default_db_path(root)
        faiss_path = default_faiss_index_path(root)
        ensure_parent_dir(db_path)
        ensure_parent_dir(faiss_path)

        result = run_pipeline_for_company_year(
            company_name_kr=args.company,
            year=args.year,
            db_path=str(db_path),
            faiss_index_path=str(faiss_path),
            cfg=cfg,
            run_benchmark=bool(args.run_benchmark),
            skip_if_exists=bool(args.skip_if_exists),
        )

        print("✅ run done:", result)
        return 0

    raise RuntimeError("unknown cmd")


if __name__ == "__main__":
    raise SystemExit(main())

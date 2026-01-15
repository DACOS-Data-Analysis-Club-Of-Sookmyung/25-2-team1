# scripts/crawl_and_store.py
# 크롤링 -> 정규화 -> 저장 파이프라인

import os
import argparse
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--company", required=True)
    p.add_argument("--year", type=int, required=True)
    args = p.parse_args()

    root = Path(__file__).resolve().parents[1]

    db_path = Path(os.environ.get("DB_PATH", str(root / "data" / "duckdb" / "dart.duckdb")))
    csv_path = Path(os.environ.get("CSV_PATH", str(root / "data" / "company_meta.csv")))
    faiss_path = Path(os.environ.get("FAISS_INDEX_PATH", str(root / "data" / "faiss" / "index.faiss")))
    model = os.environ.get("EMBED_MODEL_NAME", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

    dart_key = os.environ.get("DART_API_KEY", "").strip()
    if not dart_key:
        raise RuntimeError("DART_API_KEY가 없습니다. .env에 넣어주세요.")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    faiss_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) seed (필요하면 매번 돌려도 되는데, 보통은 최초 1회만)
    from src.seed_market import seed_market_from_csv
    seed_market_from_csv(str(db_path), str(csv_path), overwrite=False)

    # 2) ingest
    from src.ingest import ingest_company_year
    report_id = ingest_company_year(
        corp_name=args.company,
        bsns_year=args.year,
        db_path=str(db_path),
        cache_dir=str(root / "data" / "cache"),
        dart_api_key=dart_key,
        skip_if_exists=True,
    )
    print("report_id =", report_id)

    # 3) embed
    from src.embed import embed_build_or_update
    embed_build_or_update(
        db_path=str(db_path),
        index_path=str(faiss_path),
        model_name=model,
        rebuild=False,
    )
    print("✅ pipeline done.")

if __name__ == "__main__":
    main()

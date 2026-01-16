# scripts/run_seed_market.py
from __future__ import annotations

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

# ✅ 프로젝트 루트를 sys.path에 추가 (Windows에서 필수로 안전)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()


def main():
    p = argparse.ArgumentParser(description="Seed market_data / benchmark_map from CSV")
    p.add_argument("--overwrite", action="store_true", help="market_data/benchmark_map 테이블 DROP 후 재생성")
    args = p.parse_args()

    db_path = Path(os.environ.get("DB_PATH", str(ROOT / "data" / "duckdb" / "dart.duckdb")))
    csv_path = Path(os.environ.get("CSV_PATH", str(ROOT / "data" / "benchmark_results.csv")))

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    from src.seed_market import seed_market_from_csv
    seed_market_from_csv(
        db_path=str(db_path),
        csv_path=str(csv_path),
        overwrite=bool(args.overwrite),
    )

    print("✅ seed_market done")
    print("DB :", db_path)
    print("CSV:", csv_path)
    print("overwrite:", bool(args.overwrite))


if __name__ == "__main__":
    main()

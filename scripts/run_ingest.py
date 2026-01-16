# scripts/run_ingest.py
# 기업명/연도 입력 -> (옵션) seed -> (옵션) overwrite report -> target ingest -> benchmark ingest -> (옵션) QC -> DB 저장

from __future__ import annotations

import os
import sys
import argparse
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()


def _resolve_paths(root: Path):
    db_path = Path(os.environ.get("DB_PATH", str(root / "data" / "duckdb" / "dart.duckdb")))
    csv_path = Path(os.environ.get("CSV_PATH", str(root / "data" / "company_meta.csv")))
    cache_dir = Path(os.environ.get("CACHE_DIR", str(root / "data" / "cache")))
    return db_path, csv_path, cache_dir


def _resolve_report_id_for_company_year(
    db_path: Path,
    dart_api_key: str,
    company_name_kr: str,
    year: int,
    window_days: int,
    reprt_code: str,
) -> tuple[str, str, str]:
    """
    ingest_company_year() 내부에서 생성되는 report_id를 "동일한 방식"으로 미리 계산한다.
    return: (corp_code8, rcept_no, report_id)
    """
    import duckdb
    import OpenDartReader
    from src.ingest import get_target_meta_from_db
    from src.utils.ids import stable_id
    from src.utils.dart import find_business_report_rcept_no_odr

    con = duckdb.connect(str(db_path))
    try:
        meta = get_target_meta_from_db(con, company_name_kr, int(year))
        corp_code8 = meta["corp_code"]
        rcept_date = int(meta["asof_date"])
    finally:
        con.close()

    dart = OpenDartReader(dart_api_key)

    rcept_no = find_business_report_rcept_no_odr(
        dart=dart,
        corp_code=corp_code8,
        bsns_year=int(year),
        rcept_date=int(rcept_date),
        window_days=int(window_days),
        reprt_code=str(reprt_code),
    )

    report_id = stable_id(corp_code8, str(int(year)), str(rcept_no))
    return corp_code8, str(rcept_no), report_id


def _delete_report_if_exists(db_path: Path, report_id: str) -> bool:
    import duckdb
    from src.ingest import delete_report

    con = duckdb.connect(str(db_path))
    try:
        exists = con.execute("SELECT 1 FROM reports WHERE report_id=?", [report_id]).fetchone()
        if not exists:
            return False
        delete_report(con, report_id)
        return True
    finally:
        con.close()


def main():
    p = argparse.ArgumentParser(description="Ingest target + benchmark reports into DuckDB")
    p.add_argument("--company", required=True, help="기업명(한글) 예: 삼성전자")
    p.add_argument("--year", type=int, required=True, help="사업연도 예: 2024")

    # seed 옵션
    p.add_argument("--seed-market", action="store_true", help="CSV로 market_data/benchmark_map 적재 수행")
    p.add_argument("--overwrite-market", action="store_true", help="seed 시 DROP 후 재생성")

    # ingest 옵션
    p.add_argument("--window-days", type=int, default=14, help="rcept_date 기준 검색 윈도우(일)")
    p.add_argument("--reprt-code", default="11011", help="사업보고서 reprt_code (기본 11011)")
    p.add_argument("--overwrite-report", action="store_true", help="기존 report_id가 있으면 delete 후 재-ingest")
    p.add_argument("--no-benchmark", action="store_true", help="벤치마크 기업 ingest 스킵")

    # QC 옵션
    p.add_argument("--qc", action="store_true", help="ingest 후 QC 수행")

    args = p.parse_args()

    root = Path(__file__).resolve().parents[1]
    db_path, csv_path, cache_dir = _resolve_paths(root)

    dart_key = (os.environ.get("DART_API_KEY", "") or "").strip()
    if not dart_key:
        raise RuntimeError("DART_API_KEY가 없습니다. .env에 넣어주세요.")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # ✅ skip 정책: overwrite-report면 스킵하면 안 됨
    skip_if_exists = not bool(args.overwrite_report)

    # 0) (선택) seed
    if args.seed_market:
        from src.seed_market import seed_market_from_csv

        seed_market_from_csv(
            db_path=str(db_path),
            csv_path=str(csv_path),
            overwrite=bool(args.overwrite_market),
        )
        print(f"✅ seeded market tables from {csv_path.name} (overwrite={bool(args.overwrite_market)})")

    # 1) (선택) overwrite-report: target pre-delete
    if args.overwrite_report:
        try:
            _corp_code8, _rcept_no, _rid = _resolve_report_id_for_company_year(
                db_path=db_path,
                dart_api_key=dart_key,
                company_name_kr=args.company,
                year=int(args.year),
                window_days=int(args.window_days),
                reprt_code=str(args.reprt_code),
            )
            deleted = _delete_report_if_exists(db_path, _rid)
            if deleted:
                print(f"🧹 overwrite-report: deleted existing target report_id={_rid}")
            else:
                print(f"ℹ️ overwrite-report: target report_id not found (no delete) ({_rid})")
        except Exception as e:
            print(f"⚠️ overwrite-report(target) pre-delete failed, will continue ingest anyway: {e}")

    # 2) target ingest
    from src.ingest import ingest_company_year

    print(f"\n🚀 ingest target: {args.company} ({args.year})")
    target_report_id = ingest_company_year(
        corp_name=args.company,
        bsns_year=int(args.year),
        db_path=str(db_path),
        cache_dir=str(cache_dir),
        dart_api_key=dart_key,
        window_days=int(args.window_days),
        reprt_code=str(args.reprt_code),
        skip_if_exists=skip_if_exists,
    )
    print("✅ target report_id =", target_report_id)

    # 3) benchmark ingest
    bench_report_id = None
    if not args.no_benchmark:
        import duckdb
        from src.ingest import get_target_meta_from_db, get_benchmark_company_name_from_db

        con = duckdb.connect(str(db_path))
        try:
            target_meta = get_target_meta_from_db(con, args.company, int(args.year))
            bench_name = get_benchmark_company_name_from_db(con, target_meta["corp_code"], int(args.year))
        finally:
            con.close()

        if not bench_name:
            print("ℹ️ benchmark_map에 벤치 정보가 없어 benchmark ingest를 건너뜁니다.")
        else:
            # (선택) overwrite-report: benchmark pre-delete
            if args.overwrite_report:
                try:
                    _corp_code8, _rcept_no, _rid = _resolve_report_id_for_company_year(
                        db_path=db_path,
                        dart_api_key=dart_key,
                        company_name_kr=str(bench_name),
                        year=int(args.year),
                        window_days=int(args.window_days),
                        reprt_code=str(args.reprt_code),
                    )
                    deleted = _delete_report_if_exists(db_path, _rid)
                    if deleted:
                        print(f"🧹 overwrite-report: deleted existing benchmark report_id={_rid}")
                    else:
                        print(f"ℹ️ overwrite-report: benchmark report_id not found (no delete) ({_rid})")
                except Exception as e:
                    print(f"⚠️ overwrite-report(benchmark) pre-delete failed, will continue ingest anyway: {e}")

            print(f"\n🚀 ingest benchmark: {bench_name} ({args.year})")
            bench_report_id = ingest_company_year(
                corp_name=str(bench_name),
                bsns_year=int(args.year),
                db_path=str(db_path),
                cache_dir=str(cache_dir),
                dart_api_key=dart_key,
                window_days=int(args.window_days),
                reprt_code=str(args.reprt_code),
                skip_if_exists=skip_if_exists,
            )
            print("✅ benchmark report_id =", bench_report_id)

    # 4) (선택) QC
    if args.qc:
        import duckdb
        from src.validate import validate_ingest_report, validate_market_tables

        con = duckdb.connect(str(db_path))
        try:
            print("\n🧪 QC: market tables")
            market_qc = validate_market_tables(con)
            print("- market_data_rows:", market_qc["market_data_rows"])
            print("- benchmark_map_rows:", market_qc["benchmark_map_rows"])

            if len(market_qc["dup_market"]) > 0:
                print("⚠️ dup_market (top rows):")
                print(market_qc["dup_market"].head(10))

            if len(market_qc["dup_map"]) > 0:
                print("⚠️ dup_map (top rows):")
                print(market_qc["dup_map"].head(10))

            if len(market_qc["missing_bench_in_market_data"]) > 0:
                print("⚠️ missing_bench_in_market_data (top rows):")
                print(market_qc["missing_bench_in_market_data"].head(10))

            print("\n🧪 QC: ingest target report")
            tgt = validate_ingest_report(con, target_report_id)
            print(tgt["sections"])
            print(tgt["tables"])
            print(tgt["chunks"])
            print("- fs_facts_cnt:", tgt["fs_facts_cnt"])
            print("- note_links_cnt:", tgt["note_links_cnt"])

            if bench_report_id:
                print("\n🧪 QC: ingest benchmark report")
                b = validate_ingest_report(con, bench_report_id)
                print(b["sections"])
                print(b["tables"])
                print(b["chunks"])
                print("- fs_facts_cnt:", b["fs_facts_cnt"])
                print("- note_links_cnt:", b["note_links_cnt"])
        finally:
            con.close()

    print("\n✅ ingest done.")
    print("DB :", db_path)
    print("cache:", cache_dir)
    print("target_report_id   :", target_report_id)
    print("benchmark_report_id:", bench_report_id)


if __name__ == "__main__":
    main()

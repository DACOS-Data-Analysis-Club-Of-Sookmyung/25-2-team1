# 오케스트레이터: meta→rcept_no→ingest→embed, benchmark 포함)
# src/pipeline.py
from __future__ import annotations
import os
from dataclasses import dataclass
import duckdb

from .ingest import (
    init_db, ensure_table_schema,
    delete_report, ingest_one_report_xml,
    get_target_meta_from_db, get_benchmark_company_name_from_db
)
from .embed import build_or_update_faiss_from_db
from .utils.dart import fetch_document_xml_texts, pick_xml_with_iii, find_business_report_rcept_no_odr

@dataclass
class PipelineConfig:
    api_key: str
    embed_model_name: str
    reprt_code_biz: str
    chunk_size: int
    chunk_overlap: int

def _run_single_company_year(
    con: duckdb.DuckDBPyConnection,
    company_name_kr: str,
    year: int,
    faiss_index_path: str,
    cfg: PipelineConfig,
    window_days: int = 14,
    delete_existing: bool = False,
    rebuild_faiss_after_delete: bool = False,
    skip_if_exists: bool = True,
) -> str:
    meta = get_target_meta_from_db(con, company_name_kr, year)
    corp_code = meta["corp_code"]
    corp_name = meta["corp_name_kr"]
    rcept_date = meta["asof_date"]

    import OpenDartReader
    dart = OpenDartReader(cfg.api_key)

    rcept_no = find_business_report_rcept_no_odr(
        dart=dart,
        corp_code=corp_code,
        bsns_year=int(year),
        rcept_date=int(rcept_date),
        window_days=window_days,
        reprt_code=cfg.reprt_code_biz,
    )

    report_id = __import__("src.utils.ids", fromlist=["stable_id"]).stable_id(corp_code, str(int(year)), str(rcept_no))

    if skip_if_exists:
        exists = con.execute("SELECT 1 FROM reports WHERE report_id=?", [report_id]).fetchone()
        if exists:
            print(f"⏭️ already exists, skip ingest: {corp_name}({corp_code}) {year} report_id={report_id}")
            return report_id

    if delete_existing:
        exists = con.execute("SELECT 1 FROM reports WHERE report_id=?", [report_id]).fetchone()
        if exists:
            print("🧹 deleting old report:", report_id)
            delete_report(con, report_id)
            if rebuild_faiss_after_delete:
                build_or_update_faiss_from_db(con, faiss_index_path, model_name=cfg.embed_model_name, rebuild=True)

    print(f"📥 fetch document.xml: corp={corp_name}({corp_code}) year={year} rcept_no={rcept_no}")
    xml_texts = fetch_document_xml_texts(str(rcept_no), cfg.api_key)
    xml_text = pick_xml_with_iii(xml_texts)

    print("📥 ingest sections/tables/text...")
    report_id2 = ingest_one_report_xml(
        xml_text=xml_text,
        con=con,
        corp_code=corp_code,
        corp_name=corp_name,
        bsns_year=int(year),
        rcept_no=str(rcept_no),
        chunk_size=cfg.chunk_size,
        chunk_overlap=cfg.chunk_overlap,
    )
    print("✅ report_id =", report_id2)

    print("🧠 embedding + faiss update...")
    build_or_update_faiss_from_db(con, faiss_index_path, model_name=cfg.embed_model_name, rebuild=False)
    return report_id2

def run_pipeline_for_company_year(
    company_name_kr: str,
    year: int,
    db_path: str,
    faiss_index_path: str,
    cfg: PipelineConfig,
    window_days: int = 14,
    delete_existing: bool = False,
    rebuild_faiss_after_delete: bool = False,
    run_benchmark: bool = True,
    skip_if_exists: bool = True,
):
    con = duckdb.connect(db_path)
    init_db(con)
    ensure_table_schema(con)

    target_report_id = _run_single_company_year(
        con=con,
        company_name_kr=company_name_kr,
        year=year,
        faiss_index_path=faiss_index_path,
        cfg=cfg,
        window_days=window_days,
        delete_existing=delete_existing,
        rebuild_faiss_after_delete=rebuild_faiss_after_delete,
        skip_if_exists=skip_if_exists,
    )

    out = {"target": target_report_id, "benchmark": None}

    if run_benchmark:
        target_meta = get_target_meta_from_db(con, company_name_kr, year)
        bench_company_name = get_benchmark_company_name_from_db(con, target_meta["corp_code"], year)

        if not bench_company_name:
            print("ℹ️ benchmark_map에 벤치 정보가 없어 benchmark run을 건너뜁니다.")
        else:
            print(f"\n🚀 run benchmark company too: {bench_company_name} ({year})")
            try:
                bench_report_id = _run_single_company_year(
                    con=con,
                    company_name_kr=bench_company_name,
                    year=year,
                    faiss_index_path=faiss_index_path,
                    cfg=cfg,
                    window_days=window_days,
                    delete_existing=delete_existing,
                    rebuild_faiss_after_delete=rebuild_faiss_after_delete,
                    skip_if_exists=skip_if_exists,
                )
                out["benchmark"] = bench_report_id
            except Exception as e:
                print(f"⚠️ benchmark ingest 실패: {bench_company_name} / {e}")

    con.close()
    print("✅ done.")
    return out if run_benchmark else target_report_id

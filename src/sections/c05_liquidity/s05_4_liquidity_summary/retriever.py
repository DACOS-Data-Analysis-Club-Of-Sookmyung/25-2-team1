from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY, render_T_SIMPLE, render_T2_BENCH

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir, spec_id=spec["id"], allow_missing_evidence=True)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    # 1) 비율 3종 요약
    liquidity_ratio_summary_table = render_T_SIMPLE(
        metric_rows, ["current_ratio", "quick_ratio", "cash_ratio"]
    )

    # 2) 구성요소 요약 (전기 대비 포함)
    liquidity_components_summary_table = render_T1_YOY(
        metric_rows, ["CURRENT_ASSETS", "INVENTORIES", "CASH_EQ", "CURRENT_LIABILITIES"]
    )

    # 3) 벤치마크 요약 (있으면 표, 없으면 "제공 없음")
    bench = render_T2_BENCH(metric_rows, ["current_ratio", "quick_ratio", "cash_ratio"])
    benchmark_liquidity_summary_table = bench if bench is not None else "제공 없음"

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "liquidity_ratio_summary_table": liquidity_ratio_summary_table,
        "liquidity_components_summary_table": liquidity_components_summary_table,
        "benchmark_liquidity_summary_table": benchmark_liquidity_summary_table,
        "key_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

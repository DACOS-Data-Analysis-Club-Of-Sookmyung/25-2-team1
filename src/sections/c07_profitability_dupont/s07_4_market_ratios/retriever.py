from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T_SIMPLE, render_T2_BENCH, render_T1_YOY

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(
        workdir,
        metrics_path=spec.get("metrics_path"),
        evidence_path=spec.get("evidence_path"),
    )
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    market_ratio_table = render_T_SIMPLE(metric_rows, ["per", "pbr", "psr"])

    # 구성요소: 최소 요구(주가/주식수). 시총/EPS/BPS가 있으면 upstream에서 metric_key 추가 후 확장 가능.
    market_components_table = render_T1_YOY(metric_rows, ["stock_price", "shares_outstanding"])

    bench = render_T2_BENCH(metric_rows, ["per", "pbr", "psr"])
    bench_market_ratio_table = bench if bench is not None else "제공 없음"

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "market_ratio_table": market_ratio_table,
        "market_components_table": market_components_table,
        "bench_market_ratio_table": bench_market_ratio_table,
        "market_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

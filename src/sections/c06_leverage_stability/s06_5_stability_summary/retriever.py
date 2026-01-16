from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY, render_T_SIMPLE, render_T2_BENCH

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(
        workdir,
        metrics_path=spec.get("metrics_path"),
        evidence_path=spec.get("evidence_path"),
    )
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    # 1) 비율 요약 (4개)
    stability_ratio_summary_table = render_T_SIMPLE(
        metric_rows,
        ["total_debt_ratio", "long_term_debt_ratio", "interest_coverage", "cash_coverage_ocf"],
    )

    # 2) 구성요소 요약 (전기 대비)
    stability_components_summary_table = render_T1_YOY(
        metric_rows,
        ["TOTAL_LIABILITIES", "NON_CURRENT_LIABILITIES", "EQUITY", "OP_PROFIT", "OCF"],
    )

    # 3) 벤치마크 요약
    bench = render_T2_BENCH(
        metric_rows,
        ["total_debt_ratio", "long_term_debt_ratio", "interest_coverage", "cash_coverage_ocf"],
    )
    benchmark_stability_summary_table = bench if bench is not None else "제공 없음"

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "stability_ratio_summary_table": stability_ratio_summary_table,
        "stability_components_summary_table": stability_components_summary_table,
        "benchmark_stability_summary_table": benchmark_stability_summary_table,
        "key_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

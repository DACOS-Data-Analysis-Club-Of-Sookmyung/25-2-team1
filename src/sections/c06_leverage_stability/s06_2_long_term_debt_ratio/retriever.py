from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY, render_T_SIMPLE, render_T2_BENCH

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    bs_ncl_equity_table = render_T1_YOY(metric_rows, ["NON_CURRENT_LIABILITIES", "EQUITY"])
    long_debt_ratio_table = render_T_SIMPLE(metric_rows, ["long_term_debt_ratio"])

    bench = render_T2_BENCH(metric_rows, ["long_term_debt_ratio"])
    bench_long_debt_ratio_table = bench if bench is not None else "제공 없음"

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bs_ncl_equity_table": bs_ncl_equity_table,
        "long_debt_ratio_table": long_debt_ratio_table,
        "bench_long_debt_ratio_table": bench_long_debt_ratio_table,
        "long_debt_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

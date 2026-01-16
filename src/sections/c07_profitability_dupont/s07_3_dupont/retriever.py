from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T_SIMPLE, render_T2_BENCH

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    dupont_3factor_table = render_T_SIMPLE(
        metric_rows,
        ["net_margin", "asset_turnover", "fin_leverage", "roe"],
    )

    bench = render_T2_BENCH(metric_rows, ["net_margin", "asset_turnover", "fin_leverage", "roe"])
    bench_dupont_table = bench if bench is not None else "제공 없음"

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "dupont_3factor_table": dupont_3factor_table,
        "bench_dupont_table": bench_dupont_table,
        "dupont_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

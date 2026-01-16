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

    turnover_ratio_table = render_T_SIMPLE(metric_rows, ["asset_turnover", "inventory_turnover", "ar_turnover"])
    profitability_ratio_table = render_T_SIMPLE(metric_rows, ["roa", "roe", "roc"])
    dupont_3factor_table = render_T_SIMPLE(metric_rows, ["net_margin", "asset_turnover", "fin_leverage", "roe"])
    market_ratio_table = render_T_SIMPLE(metric_rows, ["per", "pbr", "psr"])

    bench_turnover = render_T2_BENCH(metric_rows, ["asset_turnover", "inventory_turnover", "ar_turnover"])
    bench_profitability = render_T2_BENCH(metric_rows, ["roa", "roe", "roc"])
    bench_dupont = render_T2_BENCH(metric_rows, ["net_margin", "asset_turnover", "fin_leverage", "roe"])
    bench_market = render_T2_BENCH(metric_rows, ["per", "pbr", "psr"])

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "turnover_ratio_table": turnover_ratio_table,
        "profitability_ratio_table": profitability_ratio_table,
        "dupont_3factor_table": dupont_3factor_table,
        "market_ratio_table": market_ratio_table,
        "bench_turnover_table": bench_turnover if bench_turnover is not None else "제공 없음",
        "bench_profitability_table": bench_profitability if bench_profitability is not None else "제공 없음",
        "bench_dupont_table": bench_dupont if bench_dupont is not None else "제공 없음",
        "bench_market_ratio_table": bench_market if bench_market is not None else "제공 없음",
        "key_evidence_ch7": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

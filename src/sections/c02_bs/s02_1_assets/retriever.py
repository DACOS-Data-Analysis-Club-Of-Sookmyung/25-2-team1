from __future__ import annotations
from pathlib import Path
from typing import Dict, Any

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY, render_T_SIMPLE, render_T3_TRACE

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(
        workdir,
        metrics_path=spec.get("metrics_path"),
        evidence_path=spec.get("evidence_path"),
    )
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    bs_assets_table = render_T1_YOY(
        metric_rows,
        [
            "TOTAL_ASSETS",
            "CURRENT_ASSETS",
            "NON_CURRENT_ASSETS",
            "CASH_AND_CASH_EQUIVALENTS",
            "ACCOUNTS_RECEIVABLE",
            "INVENTORIES",
            "PROPERTY_PLANT_EQUIPMENT",
        ],
    )

    asset_efficiency_table = render_T_SIMPLE(
        metric_rows,
        ["ar_turnover", "inventory_turnover"],
    )

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bs_assets_table": bs_assets_table,
        "asset_efficiency_table": asset_efficiency_table,
        "assets_evidence": pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False),
    }

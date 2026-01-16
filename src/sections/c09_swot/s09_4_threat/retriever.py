from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List
import json

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY, render_T_SIMPLE

def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))

RISK_HINTS = ["위험", "리스크", "불확실", "경쟁", "규제", "소송", "환율", "금리", "원자재", "공급", "수요둔화"]

def _split_business_and_risk(evidence_rows: List[Dict[str, Any]]):
    biz, risk = [], []
    for r in evidence_rows:
        txt = (r.get("text") or "")
        if any(k in txt for k in RISK_HINTS):
            risk.append(r)
        else:
            biz.append(r)
    return biz, risk

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    bridge = _read_json(workdir / "bridge_summary.json")
    bridge_text = bridge.get("bridge_text", "")

    biz_rows, risk_rows = _split_business_and_risk(evidence_rows)
    business_context_evidence = pack_evidence(biz_rows, topk=len(biz_rows), include_tables=False) if biz_rows else "제공 없음"
    risk_context_evidence = pack_evidence(risk_rows, topk=len(risk_rows), include_tables=False) if risk_rows else "제공 없음"

    base = render_T1_YOY(metric_rows, ["TOTAL_LIABILITIES", "CURRENT_LIABILITIES", "CASH_EQ", "OCF"])
    cov  = render_T_SIMPLE(metric_rows, ["interest_coverage", "cash_coverage_ocf"])
    financial_vulnerability_table = base + "\n\n" + cov

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bridge_text": bridge_text,
        "risk_context_evidence": risk_context_evidence,
        "business_context_evidence": business_context_evidence,
        "financial_vulnerability_table": financial_vulnerability_table,
    }

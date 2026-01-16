from __future__ import annotations
from pathlib import Path
from typing import Any, Dict
import json

from src.sections._common.io import load_inputs

def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(
        workdir,
        metrics_path=spec.get("metrics_path"),
        evidence_path=spec.get("evidence_path"),
    )
    meta = inputs["meta"]

    out_dir = Path("outputs/sections")

    s10_1 = out_dir / "c10.s10_1.json"
    s10_2 = out_dir / "c10.s10_2.json"
    financial_strength_summary_text = _read_json(s10_1).get("content","").strip() if s10_1.exists() else "제공 없음"
    financial_risk_summary_text = _read_json(s10_2).get("content","").strip() if s10_2.exists() else "제공 없음"

    op_path = out_dir / "c09.s09_3.json"
    th_path = out_dir / "c09.s09_4.json"
    opportunity_text = _read_json(op_path).get("content","").strip() if op_path.exists() else "제공 없음"
    threat_text = _read_json(th_path).get("content","").strip() if th_path.exists() else "제공 없음"

    analysis_years = str(meta.get("analysis_years", 1))
    benchmark_method = str(meta.get("benchmark_method", "동일 업종 내 유사 규모 벤치마크 기업과 비교"))

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "financial_strength_summary_text": financial_strength_summary_text,
        "financial_risk_summary_text": financial_risk_summary_text,
        "opportunity_text": opportunity_text,
        "threat_text": threat_text,
        "analysis_years": analysis_years,
        "benchmark_method": benchmark_method,
    }

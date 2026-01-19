from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple
import json

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY


def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))

def _load_bridge_text(workdir: Path) -> str:
    p = workdir / "bridge_summary.json"
    if not p.exists():
        return ""
    obj = _read_json(p)
    return (obj.get("bridge_text") or "").strip()

def _filter_biz_only(evidence_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [r for r in evidence_rows if (r.get("type") or "").strip().lower() == "biz"]


# -------- metrics: strength only --------
def _flag_strength(r: Dict[str, Any]) -> bool:
    return (r.get("yoy_improved") is True) or (r.get("benchmark_improved") is True)

def _fmt(x: Any) -> str:
    return "N/A" if x is None else str(x)

def _bench_delta(r: Dict[str, Any]) -> Any:
    bv = r.get("benchmark_value")
    v = r.get("value")
    if bv is None or v is None:
        return None
    try:
        return float(v) - float(bv)
    except Exception:
        return None

def _md_table(title: str, rows: List[Dict[str, Any]], limit: int = 12) -> str:
    if not rows:
        return f"{title}: 제공 없음"
    headers = ["지표", "당기", "전기", "YoY%", "벤치", "bench_delta", "yoy_improved", "bench_improved"]
    out = [title, "| " + " | ".join(headers) + " |", "| " + " | ".join(["---"]*len(headers)) + " |"]
    for r in rows[:limit]:
        out.append("| " + " | ".join([
            str(r.get("metric_name_ko") or r.get("metric_key","")),
            _fmt(r.get("value")),
            _fmt(r.get("value_prev")),
            _fmt(r.get("yoy_pct")),
            _fmt(r.get("benchmark_value")),
            _fmt(_bench_delta(r)),
            _fmt(r.get("yoy_improved")),
            _fmt(r.get("benchmark_improved")),
        ]) + " |")
    return "\n".join(out)


def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir, spec_id=spec["id"], allow_missing_evidence=True)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]
    metrics_obj = inputs["metrics"]
    rows_list = metrics_obj.get("rows", []) or []

    bridge_text = _load_bridge_text(workdir)

    biz_rows = _filter_biz_only(evidence_rows)
    business_context_evidence = (
        pack_evidence(biz_rows, topk=min(len(biz_rows), 10), include_tables=False)
        if biz_rows else "제공 없음"
    )

    # Strength 후보만
    strength_rows = [r for r in rows_list if _flag_strength(r)]
    strength_table = _md_table("Strength 후보(개선/우위 신호)", strength_rows, limit=12)

    # 앵커 테이블(고정 축)
    anchor_profit = render_T1_YOY(metric_rows, ["REVENUE", "OP_PROFIT", "NET_INCOME"])
    anchor_cash = render_T1_YOY(metric_rows, ["OCF", "CASH_EQ"])
    anchor_stability = render_T1_YOY(metric_rows, ["TOTAL_LIABILITIES", "CURRENT_LIABILITIES", "EQUITY"])

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bridge_text": bridge_text,
        "business_context_evidence": business_context_evidence,

        "anchor_profit": anchor_profit,
        "anchor_cash": anchor_cash,
        "anchor_stability": anchor_stability,

        "strength_table": strength_table,
    }

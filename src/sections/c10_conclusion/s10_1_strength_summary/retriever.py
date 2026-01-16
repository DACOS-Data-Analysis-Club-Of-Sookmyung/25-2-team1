from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Tuple
import json

from src.sections._common.io import load_inputs
from src.sections._common.table_templates import render_T1_YOY

def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))

def _bench_delta(r: Dict[str, Any]) -> Any:
    bv = r.get("benchmark_value")
    v = r.get("value")
    if bv is None or v is None:
        return None
    try:
        return float(v) - float(bv)
    except Exception:
        return None

def _fmt(x: Any) -> str:
    return "N/A" if x is None else str(x)

def _md_table(title: str, rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return f"{title}: 제공 없음"
    headers = ["지표", "당기", "전기", "벤치", "bench_delta"]
    out = [f"{title}", "| " + " | ".join(headers) + " |", "| " + " | ".join(["---"]*len(headers)) + " |"]
    for r in rows[:12]:
        out.append("| " + " | ".join([
            str(r.get("metric_name_ko") or r.get("metric_key","")),
            _fmt(r.get("value")),
            _fmt(r.get("value_prev")),
            _fmt(r.get("benchmark_value")),
            _fmt(_bench_delta(r)),
        ]) + " |")
    return "\n".join(out)

def _split_strength(metric_rows: Dict[str, Dict[str, Any]]):
    core, aux = [], []
    for r in metric_rows.values():
        yi = r.get("yoy_improved")
        bi = r.get("benchmark_improved")
        if yi is True and bi is True:
            core.append(r); continue
        if (yi is True and bi is False) or (yi is False and bi is True):
            aux.append(r); continue
    return core, aux

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]

    bridge = _read_json(workdir / "bridge_summary.json")
    bridge_text = bridge.get("bridge_text", "")

    core, aux = _split_strength(metric_rows)

    cash_vs_profit_table = render_T1_YOY(metric_rows, ["OCF", "NET_INCOME"])

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bridge_text": bridge_text,
        "strength_core_table": _md_table("Strength(핵심 AND)", core),
        "strength_aux_table": _md_table("Strength(보조 OR)", aux),
        "cash_vs_profit_table": cash_vs_profit_table,
    }

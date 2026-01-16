from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Tuple
import json

from src.sections._common.io import load_inputs
from src.sections._common.table_templates import render_T_SIMPLE

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

def _split_weakness(metric_rows: Dict[str, Dict[str, Any]]):
    core, aux = [], []
    for r in metric_rows.values():
        yi = r.get("yoy_improved")
        bi = r.get("benchmark_improved")
        if yi is False and bi is False:
            core.append(r); continue
        if (yi is False and bi is True) or (yi is True and bi is False):
            aux.append(r); continue
    return core, aux

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(
        workdir,
        metrics_path=spec.get("metrics_path"),
        evidence_path=spec.get("evidence_path"),
    )
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]

    bridge = _read_json(workdir / "bridge_summary.json")
    bridge_text = bridge.get("bridge_text", "")

    core, aux = _split_weakness(metric_rows)

    # 9.4 결과 읽기(파일명은 inputs_spec.json의 id 기준으로 outputs/sections에 저장된다고 가정)
    out_dir = Path("outputs/sections")
    threat_path = out_dir / "c09.s09_4.json"
    threat_text = "제공 없음"
    if threat_path.exists():
        threat_text = _read_json(threat_path).get("content", "").strip()

    coverage_table = render_T_SIMPLE(metric_rows, ["interest_coverage", "cash_coverage_ocf"])

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bridge_text": bridge_text,
        "weakness_core_table": _md_table("Weakness(핵심 AND)", core),
        "weakness_aux_table": _md_table("Weakness(보조 OR)", aux),
        "threat_text": threat_text,
        "coverage_table": coverage_table,
    }

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple
import json

from src.sections._common.io import load_inputs


def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def _load_bridge_text(workdir: Path) -> str:
    p = workdir / "bridge_summary.json"
    if not p.exists():
        return ""
    obj = _read_json(p)
    return (obj.get("bridge_text") or "").strip()


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


def _md_table(title: str, rows: List[Dict[str, Any]], limit: int = 15) -> str:
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


def _split_weakness_and_or(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    core_and, aux_or = [], []
    for r in rows:
        yi = r.get("yoy_improved")
        bi = r.get("benchmark_improved")

        # 핵심 AND: 둘 다 False
        if yi is False and bi is False:
            core_and.append(r)
            continue

        # 보조 OR: False가 하나만
        if (yi is False and bi is True) or (yi is True and bi is False):
            aux_or.append(r)
            continue

        # 그 외(None 포함)는 Weakness 후보로는 제외
    return core_and, aux_or


def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir, spec_id=spec["id"], allow_missing_evidence=True)

    meta = inputs["meta"]
    metrics_obj = inputs["metrics"]
    rows_list = metrics_obj.get("rows", []) or []

    bridge_text = _load_bridge_text(workdir)

    core_and, aux_or = _split_weakness_and_or(rows_list)

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bridge_text": bridge_text,

        "weakness_core_table": _md_table("핵심 Weakness 후보(AND: yoy=False AND bench=False)", core_and, limit=12),
        "weakness_aux_table": _md_table("보조 Weakness 후보(OR: yoy/bench 중 하나만 False)", aux_or, limit=12),
    }

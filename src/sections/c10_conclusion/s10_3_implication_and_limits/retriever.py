from __future__ import annotations

from pathlib import Path
from typing import Any, Dict
import json


def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def _load_bridge_text(workdir: Path) -> str:
    p = workdir / "bridge_summary.json"
    if not p.exists():
        return ""
    obj = _read_json(p)
    return (obj.get("bridge_text") or "").strip()


def _load_prev(spec_id: str) -> str:
    p = Path("outputs/sections") / f"{spec_id}.json"
    if not p.exists():
        return ""
    obj = _read_json(p)
    return (obj.get("content") or "").strip()


def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    meta = _read_json(workdir / "meta.json")
    bridge_text = _load_bridge_text(workdir)

    s1 = _load_prev("c10.s10_1_strength_summary")
    s2 = _load_prev("c10.s10_2_risk_summary")

    prev_bundle = "\n\n".join([
        "[10.1 재무적 강점 요약]",
        s1 if s1 else "제공 없음",
        "",
        "[10.2 주의할 점 요약]",
        s2 if s2 else "제공 없음",
    ]).strip()

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bridge_text": bridge_text,
        "prev_bundle": prev_bundle,
    }

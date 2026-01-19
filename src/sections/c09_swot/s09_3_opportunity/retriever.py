from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List
import json

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY


def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def _load_bridge_text(workdir: Path) -> str:
    """
    bridge_summary.json을 읽어 최종 bridge_text를 반환.
    - 최상위 bridge_text가 있으면 그대로 사용
    - 없으면 chapters[*].chapter_text를 장 순서대로 합쳐 사용
    """
    p = workdir / "bridge_summary.json"
    if not p.exists():
        return ""

    obj = _read_json(p)

    bt = (obj.get("bridge_text") or "").strip()
    if bt:
        return bt

    chapters = obj.get("chapters")
    if not isinstance(chapters, dict):
        return ""

    def _ckey(k: str) -> int:
        try:
            return int(k)
        except Exception:
            return 10**9

    lines: List[str] = []
    for ck in sorted(chapters.keys(), key=_ckey):
        ch_obj = chapters.get(ck, {}) or {}
        ct = (ch_obj.get("chapter_text") or "").strip()
        if ct:
            lines.append(ct)

    return "\n\n".join(lines).strip()


def _filter_biz_only(evidence_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in evidence_rows:
        r_type = (r.get("type") or "").strip().lower()
        if r_type == "biz":
            out.append(r)
    return out


def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir, spec_id=spec["id"], allow_missing_evidence=True)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    bridge_text = _load_bridge_text(workdir)

    biz_rows = _filter_biz_only(evidence_rows)
    business_context_evidence = (
        pack_evidence(biz_rows, topk=len(biz_rows), include_tables=False)
        if biz_rows else "제공 없음"
    )

    financial_capacity_table = render_T1_YOY(
        metric_rows,
        ["CASH_EQ", "OCF", "TOTAL_LIABILITIES", "CURRENT_LIABILITIES"]
    )

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "bridge_text": bridge_text,
        "business_context_evidence": business_context_evidence,
        "financial_capacity_table": financial_capacity_table,
    }

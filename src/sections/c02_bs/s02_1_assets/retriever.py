from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY, render_T_SIMPLE, render_T3_TRACE


def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    자산 섹션 ctx 생성:
    - 표: 자산 항목 YoY (T1)
    - 표: 회전율 지표 (T_SIMPLE)
    - 주석근거: evidence_rows -> pack_evidence
    - trace: (현재는 데이터/매핑 미확정이므로 기본은 빈 trace로 제공)
      -> prompt.md에 {note_trace_assets}가 있으므로 키는 항상 제공해야 함.
    """
    inputs = load_inputs(workdir, spec_id=spec["id"], allow_missing_evidence=True)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]
    evidence_rows = inputs["evidence_rows"]

    # -------------------------
    # 1) 자산 항목 YoY 테이블
    # -------------------------
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

    # -------------------------
    # 2) 효율성(회전율) 테이블
    # -------------------------
    asset_efficiency_table = render_T_SIMPLE(
        metric_rows,
        ["ar_turnover", "inventory_turnover"],
    )

    # -------------------------
    # 3) 지표 → 주석 연결(trace)
    # -------------------------
    # 현재 evidence_rows에 "어떤 항목(item)과 연결되는지" 구조가 확실치 않으므로
    # 우선 빈 trace를 제공해서 KeyError를 방지한다.
    #
    # 나중에 evidence_rows에 item/metric_key 같은 필드가 들어오면 아래 trace_items를 구성해
    # render_T3_TRACE(trace_items)로 문자열을 채우면 됨.
    trace_items: List[Dict[str, Any]] = []
    note_trace_assets = render_T3_TRACE(trace_items) if trace_items else ""

    # -------------------------
    # 4) 주석 근거 텍스트
    # -------------------------
    assets_evidence = pack_evidence(
        evidence_rows,
        topk=len(evidence_rows),
        include_tables=False,
    )

    corp_name = meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code")

    return {
        "corp_name": corp_name,
        "bsns_year": meta.get("bsns_year"),
        "bs_assets_table": bs_assets_table,
        "asset_efficiency_table": asset_efficiency_table,
        "note_trace_assets": note_trace_assets,
        "assets_evidence": assets_evidence,
    }

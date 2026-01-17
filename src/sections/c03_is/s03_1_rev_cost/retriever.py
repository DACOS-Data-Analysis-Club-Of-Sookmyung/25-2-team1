from __future__ import annotations
from pathlib import Path
from typing import Any, Dict

from src.sections._common.io import load_inputs, pack_evidence
from src.sections._common.table_templates import render_T1_YOY

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    inputs = load_inputs(workdir, spec_id=spec["id"], allow_missing_evidence=True)
    meta = inputs["meta"]
    metric_rows = inputs["metric_rows"]

    # 3.1 기본 표
    is_rev_cost_table = render_T1_YOY(metric_rows, ["REVENUE", "COGS", "GROSS_PROFIT"])

    # evidence는 “이 섹션에 필요한 것만” upstream에서 넣어준다는 전제
    evidence_rows = inputs["evidence_rows"]
    rev_cost_evidence = pack_evidence(evidence_rows, topk=len(evidence_rows), include_tables=False)

    # 옵션(없으면 format() 에러 방지)
    segment_table_optional = inputs.get("segment_table_optional", "제공 없음")
    segment_evidence_optional = inputs.get("segment_evidence_optional", "제공 없음")

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "is_rev_cost_table": is_rev_cost_table,
        "rev_cost_evidence": rev_cost_evidence,
        "segment_table_optional": segment_table_optional,
        "segment_evidence_optional": segment_evidence_optional,
    }

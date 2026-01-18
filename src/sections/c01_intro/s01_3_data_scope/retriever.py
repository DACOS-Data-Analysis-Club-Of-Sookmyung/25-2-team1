from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
import json

from src.sections._common.io import load_inputs

def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    meta_path = Path(workdir) / "meta" / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    fs_overview = "\n".join([
        "- 본 보고서는 DART 사업보고서(document.xml)를 기반으로 분석 대상 기업의 정보를 구성한다.",
        "- 연결재무제표(III-2) 및 연결재무제표 주석(III-3)을 분석 범위로 한다.",
        "- 재무 수치는 사전에 정형화·검증된 데이터만 사용하며 임의 계산이나 추정은 수행하지 않는다.",
        "- 서술 근거는 입력으로 제공된 근거 텍스트 chunk를 인용하여 제시한다."
    ])

    return {
        "corp_name": meta.get("corp_name") or meta.get("corp_name_kr") or meta.get("corp_code"),
        "bsns_year": meta.get("bsns_year"),
        "fs_overview": fs_overview,
    }

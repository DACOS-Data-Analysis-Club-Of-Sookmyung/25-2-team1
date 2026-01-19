from __future__ import annotations
from pathlib import Path
from typing import Dict, Any
import json


def build_ctx(workdir: Path, spec: Dict[str, Any]) -> Dict[str, Any]:
    meta = json.loads((Path(workdir) / "meta" / "meta.json").read_text(encoding="utf-8"))

    return {
        "corp_name": meta.get("corp_name"),
        "bsns_year": meta.get("bsns_year"),
    }
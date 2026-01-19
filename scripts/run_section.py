# scripts/run_section.py
from __future__ import annotations

import os
import sys
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
import duckdb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

load_dotenv()

DEFAULT_DB = ROOT / "data" / "duckdb" / "dart.duckdb"
DEFAULT_WORKDIR = ROOT / "data" / "workdir"
SECTIONS_ROOT = ROOT / "src" / "sections"


def _load_spec(section_dir: Path) -> dict:
    spec_path = section_dir / "inputs_spec.json"
    if not spec_path.exists():
        raise FileNotFoundError(f"[ERR] inputs_spec.json not found: {spec_path}")
    return json.loads(spec_path.read_text(encoding="utf-8"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--company", required=True)
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--section", required=True, help='예: "c01_intro/s01_1_objective"')
    p.add_argument("--build", action="store_true", help="meta/metrics/evidence json 생성")
    p.add_argument("--db-path", default=str(DEFAULT_DB))
    p.add_argument("--workdir", default=str(DEFAULT_WORKDIR))
    p.add_argument("--topk", type=int, default=5, help="note 당 evidence chunk 개수")
    args = p.parse_args()

    db_path = Path(args.db_path)
    workdir_root = Path(args.workdir)

    section_dir = SECTIONS_ROOT / args.section
    print(f"[INFO] section_dir = {section_dir}")

    spec = _load_spec(section_dir)
    print(f"[INFO] loaded spec id={spec.get('id')} title={spec.get('title')}")

    con = duckdb.connect(str(db_path))
    try:
        if not args.build:
            print("[INFO] --build not set. nothing to do.")
            return

        out_base = workdir_root
        out_base.mkdir(parents=True, exist_ok=True)
        
        print(f"[INFO] out_base = {out_base}")

        (workdir_root / "meta").mkdir(parents=True, exist_ok=True)
        (workdir_root / "metrics").mkdir(parents=True, exist_ok=True)
        (workdir_root / "evidence").mkdir(parents=True, exist_ok=True)
        (workdir_root / "summary").mkdir(parents=True, exist_ok=True)

        # builders
        from src.sections._common.builders.create_meta import build_meta, save_meta_json
        from src.sections._common.builders.create_metrics import build_metrics_for_section, save_metrics_json
        from src.sections._common.builders.create_evidence import build_evidence_for_section, save_evidence_json

        sec_leaf = args.section.split("/")[-1]       # s02_1_assets
        stem = "_".join(sec_leaf.split("_")[:2])   

        # 1) meta (✅ report_id 인자 없음)
        meta = build_meta(con, corp_name_kr=args.company, bsns_year=int(args.year))
        meta_path = workdir_root / "meta" / "meta.json"
        save_meta_json(meta, meta_path)
        print(f"[OK] wrote meta: {meta_path}")

        report_id = meta.get("report_id")
        if not report_id:
            raise RuntimeError("[ERR] meta.report_id가 없습니다. 먼저 run_ingest로 reports가 적재됐는지 확인해줘.")

        # 2) metrics
        metrics_spec = spec.get("metrics_spec") or {}
        metrics_json = build_metrics_for_section(con, report_id=str(report_id), metrics_spec=metrics_spec)
        metrics_path = workdir_root / "metrics" / f"{stem}_metrics.json"
        save_metrics_json(metrics_json, metrics_path)
        print(f"[OK] wrote metrics: {metrics_path}")

        # 3) evidence
        evidence_spec = spec.get("evidence_spec")

        # ✅ section1 말고는 evidence_spec을 굳이 안 쓰고, metrics_spec 기반으로 자동 생성
        # - evidence_spec이 없고 metrics_spec이 있으면 notes_by_metrics로 생성
        if not evidence_spec and (spec.get("metrics_spec")):
            evidence_spec = {"type": "notes_by_metrics", "topk_chunks_per_note": int(args.topk)}

        # - evidence_spec이 있고 type=biz면 I/II 전체 텍스트 evidence 생성
        if evidence_spec:
            evidence_json = build_evidence_for_section(
                con=con,
                report_id=str(report_id),
                evidence_spec=evidence_spec,
                metrics_json=metrics_json,
            )
            evidence_path = workdir_root / "evidence" / f"{stem}_evidence.json"
            save_evidence_json(evidence_json, evidence_path)
            print(f"[OK] wrote evidence: {evidence_path}")
        else:
            print("[INFO] evidence_spec not found and metrics_spec empty -> skip evidence")

        print("\n✅ build done.")

    finally:
        con.close()


if __name__ == "__main__":
    main()

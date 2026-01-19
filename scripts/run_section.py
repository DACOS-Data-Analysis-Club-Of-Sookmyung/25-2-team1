# scripts/run_section.py

from __future__ import annotations

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
DEFAULT_WORKDIR = ROOT / "workdir"
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

        out_base = workdir_root / args.company / str(args.year) / args.section
        out_base.mkdir(parents=True, exist_ok=True)
        print(f"[INFO] out_base = {out_base}")

        # ✅ builders (네가 옮긴 경로로 import)
        from prompts.common.builders.create_meta import build_meta, save_meta_json
        from prompts.common.builders.create_metrics import build_metrics_for_section, save_metrics_json
        from prompts.common.builders.create_evidence import build_evidence_for_section, save_evidence_json

        # 1) meta
        meta = build_meta(con, corp_name_kr=args.company, bsns_year=int(args.year))
        meta_path = out_base / "meta" / "meta.json"
        save_meta_json(meta, meta_path)
        print(f"[OK] wrote meta: {meta_path}")

        report_id = meta.get("report_id")
        if not report_id:
            raise RuntimeError("[ERR] meta.report_id가 없습니다. 먼저 run_ingest로 reports가 적재됐는지 확인해줘.")

        # 2) metrics
        metrics_spec = spec.get("metrics_spec") or []
        metrics_json = build_metrics_for_section(con, report_id=str(report_id), metrics_spec=metrics_spec)
        metrics_path = out_base / (spec.get("metrics_path") or "metrics/metrics.json")
        save_metrics_json(metrics_json, metrics_path)
        print(f"[OK] wrote metrics: {metrics_path}")

        # 3) evidence
        evidence_spec = spec.get("evidence_spec")  # biz일 때만 의미 있음

        evidence_json = build_evidence_for_section(
            con=con,
            report_id=str(report_id),
            evidence_spec=evidence_spec,      # ✅ biz 판단에만 사용
            metrics_json=metrics_json,        # ✅ notes_by_metrics에 필수
            max_notes=8,
            topk_chunks_per_note=int(args.topk),
            topk_biz_chunks=80,
        )
        evidence_path = out_base / (spec.get("evidence_path") or "evidence/evidence.json")
        save_evidence_json(evidence_json, evidence_path)
        print(f"[OK] wrote evidence: {evidence_path}")

        print("\n✅ build done.")
    finally:
        con.close()


if __name__ == "__main__":
    main()

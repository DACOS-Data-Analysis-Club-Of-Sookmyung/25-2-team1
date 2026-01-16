# src/sections/_common/table_templates.py
from __future__ import annotations
from typing import Any, Dict, List, Optional

def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    head = "| " + " | ".join(headers) + " |"
    sep  = "| " + " | ".join(["---"] * len(headers)) + " |"
    body = "\n".join("| " + " | ".join(r) + " |" for r in rows)
    return "\n".join([head, sep, body])

def render_T1_YOY(metric_rows: Dict[str, Dict[str, Any]], metric_keys: List[str]) -> str:
    headers = ["항목", "당기", "전기", "증감액", "증감률"]
    rows = []
    for k in metric_keys:
        r = metric_rows.get(k)
        if not r: 
            continue
        rows.append([
            str(r.get("metric_name_ko","")),
            str(r.get("value","")),
            str(r.get("value_prev","")),
            str(r.get("yoy_abs","")),
            str(r.get("yoy_pct","")),
        ])
    return _md_table(headers, rows)

def render_T2_BENCH(metric_rows: Dict[str, Dict[str, Any]], metric_keys: List[str]) -> Optional[str]:
    headers = ["지표", "당기", "벤치", "벤치 대비(개선?)"]
    rows = []
    for k in metric_keys:
        r = metric_rows.get(k)
        if not r:
            continue
        if r.get("benchmark_corp_code") is None:
            continue

        bi = r.get("benchmark_improved")
        if bi is True:
            bench_txt = "개선"
        elif bi is False:
            bench_txt = "악화"
        else:
            bench_txt = "N/A"

        rows.append([
            str(r.get("metric_name_ko", "")),
            str(r.get("value", "")),
            str(r.get("benchmark_value", "")),
            bench_txt,
        ])

    if not rows:
        return None
    return _md_table(headers, rows)


def render_T_SIMPLE(metric_rows: Dict[str, Dict[str, Any]], metric_keys: List[str]) -> str:
    headers = ["지표", "당기", "전기", "증감"]
    rows = []
    for k in metric_keys:
        r = metric_rows.get(k)
        if not r:
            continue
        rows.append([
            str(r.get("metric_name_ko","")),
            str(r.get("value","")),
            str(r.get("value_prev","")),
            str(r.get("yoy_abs","")),
        ])
    return _md_table(headers, rows)

def render_T3_TRACE(trace_items: List[Dict[str, Any]]) -> str:
    # trace_items는 build_ctx에서 만들어서 넣어주는 걸 추천
    # 형식: {"item":"OCF","note_no":4,"chunk_id":"C_1203","point":"..."}
    out = []
    for t in trace_items:
        out.append(f"- [{t['item']}] → note_no={t['note_no']} → {t['point']} (근거: note_no={t['note_no']}, section_code={t.get('section_code','')}, chunk_id={t['chunk_id']})")
    return "\n".join(out)

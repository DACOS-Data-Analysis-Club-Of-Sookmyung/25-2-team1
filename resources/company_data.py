# resources/company_data.py
from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import requests


# =========================
# 0) PATHS (repo fixed)
# =========================
REPO_ROOT = Path(__file__).resolve().parents[1]

CORP_ALL_PATH = REPO_ROOT / "data" / "benchmark" / "dart_corp_codes.csv"
OUT_PATH = REPO_ROOT / "data" / "company_meta.csv"

CACHE_DIR = REPO_ROOT / "data" / "_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

RCEPT_CACHE_PATH = CACHE_DIR / "cache_rcept_dt_pairs.csv"
KRX_DAY_CACHE_DIR = CACHE_DIR / "cache_krx_days"
KRX_DAY_CACHE_DIR.mkdir(parents=True, exist_ok=True)


# 1) PARAMS (ipynb 그대로)
MKT_NAME = "KOSPI"
BIG_THRESHOLD = 5_000_000_000_000  
N_BIG, N_MID, N_SMALL = 40, 30, 30

# rate limit
DART_SLEEP = 0.12
KRX_SLEEP = 0.8

# rcept window policy: (year+1) 2~4월 우선, 실패 시 확장
RCEPT_WINDOWS = [
    ("0201", "0630"),
    ("0101", "0930"),
    ("0101", "1231"),
]




# 2) BASIC UTIL 
def fix_corp_code_8(df: pd.DataFrame, col="corp_code") -> pd.DataFrame:
    df = df.copy()
    if col in df.columns:
        df[col] = (
            df[col].astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"\D", "", regex=True)
            .str.zfill(8)
        )
    return df

def fix_stock_code_6(df: pd.DataFrame, col="stock_code") -> pd.DataFrame:
    df = df.copy()
    if col in df.columns:
        s = df[col]
        s = s.where(s.notna(), "")
        s = (
            s.astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"\D", "", regex=True)
            .str.zfill(6)
        )
        df[col] = s
    return df

def nonempty(x) -> bool:
    if x is None:
        return False
    s = str(x).strip()
    return s != "" and s.lower() not in ("nan", "none")

def is_blank(x) -> bool:
    if x is None:
        return True
    s = str(x).strip().lower()
    return (s == "") or (s == "nan") or (s == "none")

def _sleep(sec: float) -> None:
    time.sleep(sec)


# 3) KRX CLIENT 
def fetch_krx_stk_bydd_trd(auth_key: str, bas_dd: str) -> pd.DataFrame:
    """
    KRX '전 종목' 일자 시세/시총/상장주식수 dump
    """
    url = "https://data-dbg.krx.co.kr/svc/apis/sto/stk_bydd_trd"
    res = requests.get(url, headers={"AUTH_KEY": auth_key}, params={"basDd": bas_dd}, timeout=30)

    try:
        data = res.json()
    except Exception:
        data = {"_raw": res.text}

    rows = data.get("OutBlock_1")
    if res.status_code != 200 or rows is None or len(rows) == 0:
        msg = data.get("respMsg") or data.get("message") or str(data)[:300]
        raise RuntimeError(f"KRX failed. status={res.status_code}, basDd={bas_dd}, msg={msg}")

    return pd.json_normalize(rows)

def build_krx_year_end(krx_auth_key: str, year: int) -> Tuple[str, pd.DataFrame]:
    """
    12/31 휴장 대비 fallback (12/31, 12/30, ...)
    """
    cands = [f"{year}1231", f"{year}1230", f"{year}1229", f"{year}1228", f"{year}1227"]
    last_err = None
    for dd in cands:
        try:
            _sleep(KRX_SLEEP)
            df = fetch_krx_stk_bydd_trd(krx_auth_key, dd)
            return dd, df
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"build_krx_year_end failed. year={year} last_err={last_err}")

def _fallback_dates(rd: str) -> List[str]:
    base = datetime.strptime(rd, "%Y%m%d")
    offs = [0, -1, -2, 1, 2, -3, 3, -4, 4]
    return [(base + timedelta(days=o)).strftime("%Y%m%d") for o in offs]

def load_or_fetch_krx_day(krx_auth_key: str, bas_dd: str) -> Tuple[str, pd.DataFrame]:
    """
    KRX day dump 캐시 (비거래일이면 fallback 날짜로 자동 이동)
    """
    bas_dd = str(bas_dd).strip()
    for dd in _fallback_dates(bas_dd):
        cache_path = KRX_DAY_CACHE_DIR / f"krx_{dd}.csv"
        if cache_path.exists():
            df = pd.read_csv(cache_path, dtype=str)
            return dd, df

        try:
            _sleep(KRX_SLEEP)
            df = fetch_krx_stk_bydd_trd(krx_auth_key, dd)
            df["stock_code"] = df["ISU_CD"].astype(str).str.zfill(6)
            df.to_csv(cache_path, index=False, encoding="utf-8-sig")
            return dd, df
        except Exception:
            continue

    raise RuntimeError(f"load_or_fetch_krx_day failed. bas_dd={bas_dd}")


# 4) DART CLIENT 
def dart_list_raw(dart_api_key: str, corp_code: str, bgn_de: str, end_de: str, page_no=1, page_count=100):
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key": dart_api_key,
        "corp_code": corp_code,
        "bgn_de": bgn_de,
        "end_de": end_de,
        "page_no": page_no,
        "page_count": page_count,
    }
    _sleep(DART_SLEEP)
    r = requests.get(url, params=params, timeout=30)
    return r.json()

def pick_latest_business_report(df: pd.DataFrame, bsns_year: int) -> Optional[str]:
    if df.empty:
        return None
    # 사업보고서 포함 + bsns_year 매칭
    c = df[df["report_nm"].astype(str).str.contains("사업보고서", na=False)].copy()
    if c.empty:
        return None
    c["rcept_dt"] = c["rcept_dt"].astype(str)
    c = c[c["rcept_dt"].str.match(r"^\d{8}$", na=False)]
    if c.empty:
        return None
    c = c[c["rcept_dt"].str[:4].astype(int) == int(bsns_year)]
    if c.empty:
        return None
    c = c.sort_values("rcept_dt", ascending=False)
    return str(c.iloc[0]["rcept_dt"])

def find_rcept_dt_windowed(dart_api_key: str, corp_code: str, bsns_year: int) -> Optional[str]:
    """
    (year+1) 2~6월부터 탐색, 실패 시 확장 (ipynb policy)
    """
    corp_code = str(corp_code).zfill(8)
    for mmdd1, mmdd2 in RCEPT_WINDOWS:
        bgn_de = f"{bsns_year}{mmdd1}"
        end_de = f"{bsns_year}{mmdd2}"

        page = 1
        items_all = []
        while True:
            js = dart_list_raw(dart_api_key, corp_code, bgn_de=bgn_de, end_de=end_de, page_no=page, page_count=100)
            if js.get("status") != "000":
                break
            items = js.get("list") or []
            if not items:
                break
            items_all.extend(items)
            df = pd.DataFrame(items)
            hit = pick_latest_business_report(df, bsns_year)
            if hit:
                return hit
            page += 1
            if page > 6:
                break

        if items_all:
            df = pd.DataFrame(items_all)
            hit = pick_latest_business_report(df, bsns_year)
            if hit:
                return hit

    return None

def load_or_build_rcept_cache(
    dart_api_key: str,
    pairs: pd.DataFrame,
    refresh: bool = False,
) -> pd.DataFrame:
    """
    pairs: corp_code, year
    cache: corp_code, year -> rcept_date
    """
    if RCEPT_CACHE_PATH.exists() and not refresh:
        cache_df = pd.read_csv(RCEPT_CACHE_PATH, dtype=str)
    else:
        cache_df = pd.DataFrame(columns=["corp_code", "year", "rcept_date"])

    cache_df = fix_corp_code_8(cache_df, "corp_code")
    cache_df["year"] = cache_df["year"].astype(str)

    have = set(zip(cache_df["corp_code"], cache_df["year"]))

    new_rows = []
    for r in pairs.itertuples(index=False):
        cc = str(r.corp_code).zfill(8)
        y = str(int(r.year))
        if (cc, y) in have:
            continue

        rd = None
        try:
            rd = find_rcept_dt_windowed(dart_api_key, cc, int(y))
        except Exception:
            rd = None

        new_rows.append({"corp_code": cc, "year": y, "rcept_date": rd})
        have.add((cc, y))

    if new_rows:
        cache_df = pd.concat([cache_df, pd.DataFrame(new_rows)], ignore_index=True)
        cache_df.to_csv(RCEPT_CACHE_PATH, index=False, encoding="utf-8-sig")

    return cache_df


# 5) STEP A: eligible pool 
def build_eligible_pool_from_enriched(
    dart_api_key: str,
    krx_auth_key: str,
    corp_all_path: Path,
    years=(2023, 2024),
    require_krx_price_shares: bool = True,  
) -> pd.DataFrame:
    corp_all = pd.read_csv(corp_all_path, dtype=str)
    corp_all = fix_corp_code_8(corp_all, "corp_code")
    corp_all = fix_stock_code_6(corp_all, "stock_code")

    base = corp_all[
        corp_all["corp_code"].notna()
        & corp_all["corp_name"].notna()
        & corp_all["corp_eng_name"].notna()
        & (corp_all["corp_name"].astype(str).str.strip() != "")
        & (corp_all["corp_eng_name"].astype(str).str.strip() != "")
        & corp_all["stock_code"].astype(str).str.match(r"^\d{6}$", na=False)
        & (corp_all["stock_code"] != "000000")
    ].copy()
    print("[eligible] base after meta+stock_code filter:", base.shape)

    # rcept cache
    pairs = []
    for cc in base["corp_code"].unique():
        for y in years:
            pairs.append({"corp_code": cc, "year": y})
    pairs = pd.DataFrame(pairs)

    rcache = load_or_build_rcept_cache(dart_api_key, pairs, refresh=False)
    rcache["corp_code"] = rcache["corp_code"].astype(str).str.zfill(8)

    wide = (
        rcache.pivot(index="corp_code", columns="year", values="rcept_date")
        .reset_index()
        .rename(columns={str(years[0]): f"rcept_{years[0]}", str(years[1]): f"rcept_{years[1]}"})
    )
    out = base.merge(wide, on="corp_code", how="left")

    # rcept 유효성
    for y in years:
        col = f"rcept_{y}"
        out[col] = out[col].astype(str).str.strip()
        out = out[~out[col].str.lower().isin(["none", "nan", ""])]
        out = out[out[col].str.match(r"^\d{8}$", na=False)]
    print("[eligible] after rcept filter:", out.shape)

    if require_krx_price_shares:
        ok_codes = []
        total = len(out)
        for i, row in enumerate(out.itertuples(index=False), 1):
            cc = str(row.corp_code).zfill(8)
            sc = str(row.stock_code).zfill(6)

            ok = True
            for y in years:
                rd = getattr(row, f"rcept_{y}")
                actual_dd, daydf = load_or_fetch_krx_day(krx_auth_key, rd)
                hit = daydf[daydf["stock_code"] == sc]
                if hit.empty:
                    ok = False
                    break
                p = pd.to_numeric(hit.iloc[0].get("TDD_CLSPRC"), errors="coerce")
                s = pd.to_numeric(hit.iloc[0].get("LIST_SHRS"), errors="coerce")
                if pd.isna(p) or pd.isna(s):
                    ok = False
                    break

            if ok:
                ok_codes.append(cc)

            if i % 50 == 0 or i == total:
                print(f"[eligible krx-check] {i}/{total}")

        out = out[out["corp_code"].isin(set(ok_codes))].copy()
        print("[eligible] after KRX price/shares check:", out.shape)

    eligible = out[[
        "corp_code", "corp_name", "corp_eng_name", "stock_code",
        f"rcept_{years[0]}", f"rcept_{years[1]}",
    ]].rename(columns={
        "corp_name": "corp_name_kr",
        "corp_eng_name": "corp_name_en",
    }).drop_duplicates()

    return eligible


# 6) STEP B: sample 40/30/30 from eligible 
def sample_100_from_eligible(
    krx_auth_key: str,
    eligible: pd.DataFrame,
    year_for_mktcap: int = 2024,
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    # KRX 연말 시총 붙이기
    asof, df_krx = build_krx_year_end(krx_auth_key, year_for_mktcap)
    df_krx["stock_code"] = df_krx["ISU_CD"].astype(str).str.zfill(6)
    df_krx["MKTCAP"] = pd.to_numeric(df_krx["MKTCAP"], errors="coerce")

    base = eligible.merge(df_krx[["stock_code", "MKTCAP", "MKT_NM"]], on="stock_code", how="left")
    base = base[(base["MKTCAP"].notna()) & (base["MKT_NM"] == MKT_NAME)].copy()
    print("KRX asof:", asof, "eligible+MKTCAP:", base.shape)

    large_cand = base[base["MKTCAP"] >= BIG_THRESHOLD].sort_values("MKTCAP", ascending=False)
    mid_cand   = base[base["MKTCAP"] <  BIG_THRESHOLD].sort_values("MKTCAP", ascending=False)
    small_cand = base[base["MKTCAP"] <  BIG_THRESHOLD].sort_values("MKTCAP", ascending=True)

    used = set()

    def pick_top(df: pd.DataFrame, n: int) -> List[str]:
        out_codes = []
        for _, r in df.iterrows():
            cc = str(r["corp_code"]).zfill(8)
            if cc in used:
                continue
            used.add(cc)
            out_codes.append(cc)
            if len(out_codes) >= n:
                break
        return out_codes

    picked_large = pick_top(large_cand, N_BIG)
    picked_mid   = pick_top(mid_cand,   N_MID)
    picked_small = pick_top(small_cand, N_SMALL)

    if len(picked_large) < N_BIG: raise RuntimeError("large 부족")
    if len(picked_mid) < N_MID:   raise RuntimeError("mid 부족")
    if len(picked_small) < N_SMALL: raise RuntimeError("small 부족")

    picked_codes = picked_large + picked_mid + picked_small

    code_to_scale: Dict[str, str] = {}
    for c in picked_large: code_to_scale[c] = "large"
    for c in picked_mid:   code_to_scale[c] = "mid"
    for c in picked_small: code_to_scale[c] = "small"

    picked_df = base[base["corp_code"].isin(picked_codes)].copy()
    print("picked:", pd.Series(list(code_to_scale.values())).value_counts().to_dict())
    return picked_df, code_to_scale


# 7) STEP C: build final 2y rows 
def build_target_2y_rows(
    krx_auth_key: str,
    picked_df: pd.DataFrame,
    code_to_scale: Dict[str, str],
    years=(2023, 2024),
) -> pd.DataFrame:
    rows = []
    for cc in picked_df["corp_code"].astype(str).str.zfill(8).unique():
        one = picked_df[picked_df["corp_code"].astype(str).str.zfill(8) == cc].iloc[0]
        sc = str(one["stock_code"]).zfill(6)
        scale = code_to_scale[cc]

        nm_kr = str(one.get("corp_name_kr") or "")
        nm_en = str(one.get("corp_name_en") or "")

        r23 = str(one[f"rcept_{years[0]}"]).strip()
        r24 = str(one[f"rcept_{years[1]}"]).strip()

        # 2023
        a23, day23 = load_or_fetch_krx_day(krx_auth_key, r23)
        hit23 = day23[day23["stock_code"] == sc]
        if hit23.empty:
            raise RuntimeError(f"KRX day miss (2023): {nm_kr} {sc} rcept={r23}")
        p23 = float(hit23.iloc[0]["TDD_CLSPRC"])
        s23 = float(hit23.iloc[0]["LIST_SHRS"])

        # 2024
        a24, day24 = load_or_fetch_krx_day(krx_auth_key, r24)
        hit24 = day24[day24["stock_code"] == sc]
        if hit24.empty:
            raise RuntimeError(f"KRX day miss (2024): {nm_kr} {sc} rcept={r24}")
        p24 = float(hit24.iloc[0]["TDD_CLSPRC"])
        s24 = float(hit24.iloc[0]["LIST_SHRS"])

        rows.append({
            "corp_code": cc,
            "corp_name_kr": nm_kr,
            "corp_name_en": nm_en,
            "stock_code": sc,
            "year": years[0],
            "rcept_date": r23,
            "stock_price": p23,
            "shares_outstanding": s23,
            "scale": scale,
        })
        rows.append({
            "corp_code": cc,
            "corp_name_kr": nm_kr,
            "corp_name_en": nm_en,
            "stock_code": sc,
            "year": years[1],
            "rcept_date": r24,
            "stock_price": p24,
            "shares_outstanding": s24,
            "scale": scale,
        })

    out = pd.DataFrame(rows)
    out = fix_corp_code_8(out, "corp_code")
    out = fix_stock_code_6(out, "stock_code")
    return out

# BENCHMARK MAP 
BENCHMARK_BY_TARGET: Dict[str, str] = {
    # ===== 반도체/전자 =====
    "삼성전자": "SK하이닉스",
    "SK하이닉스": "삼성전자",
    "LG전자": "삼성전자",
    "LG디스플레이": "LG이노텍",
    "LG이노텍": "LG디스플레이",
    "삼성SDI": "LG에너지솔루션",
    "LG에너지솔루션": "삼성SDI",
    "HD현대일렉트릭": "엘에스일렉트릭",
    "엘에스일렉트릭": "HD현대일렉트릭",

    # ===== 2차전지/소재 =====
    "포스코퓨처엠": "에코프로머티",
    "에코프로머티": "포스코퓨처엠",

    # ===== 바이오/제약/헬스케어 =====
    "삼성바이오로직스": "셀트리온",
    "셀트리온": "삼성바이오로직스",
    "한미약품": "유한양행",
    "SK바이오사이언스": "녹십자",
    "차AI헬스케어": "포스코DX",

    # ===== 자동차/부품/모빌리티 =====
    "현대자동차": "기아",
    "기아": "현대자동차",
    "현대모비스": "HL만도",
    "두산밥캣": "HD현대건설기계",
    "현대오토에버": "포스코DX",

    # ===== 플랫폼/인터넷/게임 =====
    "NAVER": "카카오",
    "카카오": "NAVER",
    "크래프톤": "엔씨소프트",
    "엔씨소프트": "크래프톤",
    "넷마블": "엔씨소프트",
    "카카오페이": "포스코DX",

    # ===== 통신 =====
    "SK텔레콤": "케이티",
    "케이티": "SK텔레콤",
    "LG유플러스": "케이티",

    # ===== 금융 =====
    "KB금융": "신한지주",
    "신한지주": "KB금융",
    "하나금융지주": "(주)우리금융지주",
    "우리금융지주": "하나금융지주",
    "NH투자증권": "미래에셋증권",
    "미래에셋증권": "NH투자증권",
    "삼성증권": "키움증권",
    "메리츠금융지주": "한국금융지주",
    "한국금융지주": "메리츠금융지주",
    "삼성생명": "한화생명",
    "삼성화재해상보험": "DB손해보험",
    "삼성카드": "KB금융",
    "기업은행": "KB금융",
}


# 8) STEP D: benchmark meta (내부+외부) + bench KRX
def get_corp_info_by_name(dart_api_key: str, corp_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    DART list.json을 corp_name 검색으로 사용 (ipynb cell25 유지)
    """
    url = "https://opendart.fss.or.kr/api/list.json"
    params = {
        "crtfc_key": dart_api_key,
        "corp_name": corp_name,
        "bgn_de": "20230101",
    }
    try:
        _sleep(DART_SLEEP)
        res = requests.get(url, params=params, timeout=30).json()
        if res.get("status") == "000":
            for item in res.get("list", []):
                if item["corp_name"].replace(" ", "") == corp_name.replace(" ", ""):
                    return item["corp_code"], item["stock_code"].strip()
    except Exception:
        pass
    return None, None


def build_all_benchmark_meta(
    dart_api_key: str,
    corp_all: pd.DataFrame,
    eligible: pd.DataFrame,
    target_2y: pd.DataFrame,
    years=(2023, 2024),
) -> pd.DataFrame:
    """
    target_2y에 매핑될 벤치(내부+외부)의 (bench_corp_code, bench_stock_code, bench_rcept_date) 생성
    """
    target_names = set(target_2y["corp_name_kr"].unique())
    all_bench_names = set(BENCHMARK_BY_TARGET.values())
    internal_bench = all_bench_names & target_names
    external_bench = all_bench_names - target_names
    print(f"[bench] total={len(all_bench_names)} internal={len(internal_bench)} external={len(external_bench)}")

    # 내부 벤치: target_2y에서 바로 확보
    internal_meta = target_2y[["corp_name_kr", "year", "corp_code", "stock_code", "rcept_date"]].copy()
    internal_meta.columns = ["benchmark_name_kr", "year", "bench_corp_code", "bench_stock_code", "bench_rcept_date"]

    # 외부 벤치: 우선 eligible에서 찾고, 없으면 corp_all, 그래도 없으면 DART API
    # eligible: corp_name_kr, corp_code, stock_code, rcept_2023/2024 존재
    elig_map = {}
    for r in eligible.itertuples(index=False):
        elig_map[str(r.corp_name_kr).replace(" ", "")] = {
            "corp_code": str(r.corp_code).zfill(8),
            "stock_code": str(r.stock_code).zfill(6),
            str(years[0]): str(getattr(r, f"rcept_{years[0]}")),
            str(years[1]): str(getattr(r, f"rcept_{years[1]}")),
        }

    corp_all_map = {}
    for r in corp_all.itertuples(index=False):
        nm = str(getattr(r, "corp_name", "")).replace(" ", "")
        if nm and nm not in corp_all_map:
            corp_all_map[nm] = {
                "corp_code": str(getattr(r, "corp_code", "")).zfill(8),
                "stock_code": str(getattr(r, "stock_code", "")).zfill(6),
            }

    external_rows = []
    for name in external_bench:
        key = name.replace(" ", "")
        cc = sc = None
        r23 = r24 = None

        # (1) eligible에서 찾기
        if key in elig_map:
            cc = elig_map[key]["corp_code"]
            sc = elig_map[key]["stock_code"]
            r23 = elig_map[key].get(str(years[0]))
            r24 = elig_map[key].get(str(years[1]))

        # (2) corp_all에서 코드 확보 후 rcept 찾기
        if (not cc or not sc) and key in corp_all_map:
            cc = corp_all_map[key]["corp_code"]
            sc = corp_all_map[key]["stock_code"]
            if cc and cc != "00000000":
                r23 = find_rcept_dt_windowed(dart_api_key, cc, years[0])
                r24 = find_rcept_dt_windowed(dart_api_key, cc, years[1])

        # (3) DART 이름검색
        if (not cc or not sc):
            cc, sc = get_corp_info_by_name(dart_api_key, name)
            if cc:
                cc = str(cc).zfill(8)
                sc = str(sc).zfill(6) if sc else None
                r23 = find_rcept_dt_windowed(dart_api_key, cc, years[0])
                r24 = find_rcept_dt_windowed(dart_api_key, cc, years[1])

        # 행 생성
        if cc and sc and r23 and r24 and re.match(r"^\d{8}$", str(r23)) and re.match(r"^\d{8}$", str(r24)):
            external_rows.append({
                "benchmark_name_kr": name,
                "year": years[0],
                "bench_corp_code": cc,
                "bench_stock_code": str(sc).zfill(6),
                "bench_rcept_date": str(r23),
            })
            external_rows.append({
                "benchmark_name_kr": name,
                "year": years[1],
                "bench_corp_code": cc,
                "bench_stock_code": str(sc).zfill(6),
                "bench_rcept_date": str(r24),
            })
        else:
            # 실패해도 파이프라인은 계속 (나중에 결측으로 남음)
            print(f"[bench warn] cannot resolve bench: {name} (cc={cc}, sc={sc}, r23={r23}, r24={r24})")

    external_meta = pd.DataFrame(external_rows) if external_rows else pd.DataFrame(
        columns=["benchmark_name_kr","year","bench_corp_code","bench_stock_code","bench_rcept_date"]
    )

    all_bench_meta = pd.concat([internal_meta, external_meta], ignore_index=True)
    all_bench_meta = all_bench_meta.drop_duplicates(subset=["benchmark_name_kr", "year"])
    return all_bench_meta


def attach_benchmark_and_market(
    krx_auth_key: str,
    target_2y: pd.DataFrame,
    all_bench_meta: pd.DataFrame,
) -> pd.DataFrame:
    # benchmark_name_kr 붙이기
    df = target_2y.copy()
    df["benchmark_name_kr"] = df["corp_name_kr"].map(BENCHMARK_BY_TARGET)

    # bench meta 병합
    df = df.merge(all_bench_meta, on=["benchmark_name_kr", "year"], how="left")

    # bench market (유니크 요청만)
    unique_reqs = df[["bench_stock_code", "bench_rcept_date"]].dropna().drop_duplicates()
    price_map: Dict[Tuple[str, str], Tuple[float, float]] = {}

    print(f"[bench market] unique reqs: {len(unique_reqs)}")
    for r in unique_reqs.itertuples(index=False):
        sc = str(r.bench_stock_code).zfill(6)
        rd = str(r.bench_rcept_date).strip()
        if not re.match(r"^\d{8}$", rd):
            continue
        try:
            _, daydf = load_or_fetch_krx_day(krx_auth_key, rd)
            hit = daydf[daydf["stock_code"] == sc]
            if hit.empty:
                continue
            p = float(hit.iloc[0]["TDD_CLSPRC"])
            s = float(hit.iloc[0]["LIST_SHRS"])
            price_map[(sc, rd)] = (p, s)
        except Exception:
            continue

    def apply_bench(r):
        sc = str(r["bench_stock_code"]).zfill(6) if nonempty(r["bench_stock_code"]) else ""
        rd = str(r["bench_rcept_date"]).strip() if nonempty(r["bench_rcept_date"]) else ""
        key = (sc, rd)
        if key in price_map:
            p, s = price_map[key]
            return pd.Series([p, s])
        return pd.Series([np.nan, np.nan])

    df[["bench_stock_price", "bench_shares_outstanding"]] = df.apply(apply_bench, axis=1)

    # 숫자형 정리
    for c in ["stock_price", "shares_outstanding", "bench_stock_price", "bench_shares_outstanding"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


# 9) RUN 
def main() -> None:
    dart_api_key = os.getenv("DART_API_KEY", "").strip()
    krx_auth_key = os.getenv("KRX_AUTH_KEY", "").strip()
    if not dart_api_key:
        raise SystemExit("Missing env DART_API_KEY")
    if not krx_auth_key:
        raise SystemExit("Missing env KRX_AUTH_KEY")

    # corp_all load (bench resolve에도 사용)
    corp_all = pd.read_csv(CORP_ALL_PATH, dtype=str)
    corp_all = fix_corp_code_8(corp_all, "corp_code")
    corp_all = fix_stock_code_6(corp_all, "stock_code")

    # A) eligible (rcept 2y + (옵션) KRX price/shares 검증 포함)
    eligible = build_eligible_pool_from_enriched(
        dart_api_key=dart_api_key,
        krx_auth_key=krx_auth_key,
        corp_all_path=CORP_ALL_PATH,
        years=(2023, 2024),
        require_krx_price_shares=True,
    )

    # B) 100개 선정 (40/30/30)
    picked_df, code_to_scale = sample_100_from_eligible(
        krx_auth_key=krx_auth_key,
        eligible=eligible,
        year_for_mktcap=2024,
    )

    # C) 타겟 2년치 row 생성 (주가/주식수 포함)
    target_2y = build_target_2y_rows(
        krx_auth_key=krx_auth_key,
        picked_df=picked_df,
        code_to_scale=code_to_scale,
        years=(2023, 2024),
    )

    # D) 벤치(내부+외부) 메타 만들고 bench 시장데이터까지 붙이기
    all_bench_meta = build_all_benchmark_meta(
        dart_api_key=dart_api_key,
        corp_all=corp_all,
        eligible=eligible,
        target_2y=target_2y,
        years=(2023, 2024),
    )

    final = attach_benchmark_and_market(
        krx_auth_key=krx_auth_key,
        target_2y=target_2y,
        all_bench_meta=all_bench_meta,
    )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")

    print("[OK] saved:", OUT_PATH)
    print("shape:", final.shape)
    print("scale counts:", final["scale"].value_counts().to_dict())
    print("bench missing:", int(final["bench_corp_code"].isna().sum()))


if __name__ == "__main__":
    main()

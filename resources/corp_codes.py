import os
import time
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv  
import dart_fss as dart

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

DART_API_KEY = os.getenv("DART_API_KEY")
SAVE_PATH = os.path.join(BASE_DIR, "data", "benchmark", "dart_corp_codes.csv")

class CorpCodeManager:
    """
    DART 기업 고유번호(corp_code) 목록을 수집하고 영문명을 보강하는 클래스
    """
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("[!] DART_API_KEY가 설정되지 않았습니다. .env 파일을 확인해주세요.")
        
        dart.set_api_key(api_key)
        self.required_cols = ["corp_code", "corp_name", "corp_eng_name", "stock_code", "modify_date"]
        
        os.makedirs(os.path.dirname(SAVE_PATH), exist_ok=True)

    def _standardize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터프레임 컬럼 및 데이터 형식 표준화"""
        if "corp_eng_name" not in df.columns:
            df["corp_eng_name"] = ""
            
        for col in self.required_cols:
            if col not in df.columns:
                df[col] = ""
        
        df = df[self.required_cols].copy()
        # 자릿수 맞추기 (Zero-filling)
        df["stock_code"] = df["stock_code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
        df["corp_code"] = df["corp_code"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(8)
        
        # 종목코드가 없는 경우(비상장사 등) 처리
        df.loc[df["stock_code"] == "000000", "stock_code"] = ""
        return df

    def get_or_load_base_list(self, refresh: bool = False, max_age_days: int = 30) -> pd.DataFrame:
        """캐시 로드 또는 API 수집"""
        should_fetch = refresh or not os.path.exists(SAVE_PATH)
        
        if not should_fetch:
            mtime = datetime.fromtimestamp(os.path.getmtime(SAVE_PATH))
            if datetime.now() - mtime > timedelta(days=max_age_days):
                should_fetch = True

        if should_fetch:
            print("[*] DART API에서 전체 기업 목록 수집 중...")
            try:
                corp_list = dart.api.filings.get_corp_code()
                df = pd.DataFrame(corp_list)
                df = self._standardize_df(df)
                df.to_csv(SAVE_PATH, index=False, encoding="utf-8-sig")
                return df
            except Exception as e:
                print(f"[!] API 호출 실패: {e}")
                if os.path.exists(SAVE_PATH):
                    return pd.read_csv(SAVE_PATH, dtype=str)
                raise
        else:
            print(f"[*] 기존 캐시 로드: {SAVE_PATH}")
            return pd.read_csv(SAVE_PATH, dtype=str)

    def enrich_english_names(self, df: pd.DataFrame, sleep_sec: float = 0.15, max_rows: int = None) -> pd.DataFrame:
        """기업개요 API를 통한 영문명 보강"""
        df = self._standardize_df(df)
        mask = (df["corp_eng_name"].isna()) | (df["corp_eng_name"].astype(str).str.strip() == "")
        target_indices = df[mask].index
        
        if max_rows:
            target_indices = target_indices[:max_rows]
            
        if len(target_indices) == 0:
            print("[*] 보강할 대상이 없습니다.")
            return df

        print(f"[*] 영문명 보강 시작 (대상: {len(target_indices)}건)...")
        
        for i, idx in enumerate(target_indices, 1):
            corp_code = df.at[idx, "corp_code"]
            try:
                info = dart.api.company.get_company_info(corp_code=corp_code)
                eng_name = info.get("corp_name_eng") or info.get("corp_eng_name") or info.get("corp_name_en") or ""
                df.at[idx, "corp_eng_name"] = str(eng_name).strip()
            except Exception:
                pass

            time.sleep(sleep_sec)
            if i % 100 == 0:
                print(f"    - 진행 중... ({i}/{len(target_indices)})")
                df.to_csv(SAVE_PATH, index=False, encoding="utf-8-sig")

        df.to_csv(SAVE_PATH, index=False, encoding="utf-8-sig")
        print(f"[*] 보강 완료: {SAVE_PATH}")
        return df

if __name__ == "__main__":
    manager = CorpCodeManager(DART_API_KEY)    
    corp_df = manager.get_or_load_base_list(refresh=False)
    final_df = manager.enrich_english_names(corp_df, max_rows=None)
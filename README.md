# 25-2-team1
생성형 AI 1팀
팀원 : 김소영, 이소윤, 조예린

# 📊 DART 사업보고서 기반 재무분석 AI 리포트 시스템
프로젝트 소개 PPT 를 참고해주세요 (https://www.canva.com/design/DAG-mGeuf7Y/vc07sHBVbDhlRSgnDJ1EgQ/view?utm_content=DAG-mGeuf7Y&utm_campaign=designshare&utm_medium=link2&utm_source=uniquelinks&utlId=h9631bc6555)

## 1. 프로젝트 개요
금융감독원 DART 사업보고서를 기반으로 기업의 재무 상태를 정량·정성적으로 분석하고,
근거(citation)를 포함한 초보투자자를 위한 AI 재무 리포트를 생성하는 시스템

## 2. 핵심 구현 
1) 정량 데이터와 정성 데이터의 분리 저장
- 사업 보고서 내 I. 회사의 개요, II. 사업의 내용, III-2.연결재무제표, III-3.연결재무제표 주석을 크롤링
- 텍스트 데이터와 value(값) 데이터를 분리하여 저장, table로 매핑

2) 정성 데이터를 근거 기반으로 한 LLM Agent

- 연결재무제표 주석(III-3) 텍스트를 섹션/청크 단위로 분리

- 리포트 요약 시 관련 주석 텍스트 + 표 참조를 근거로 제공

3) 계산 및 검증 파이프라인
- 재무 분석에 필요한 추가적인 값을 SQL 기반으로 계산
- 계산 결과를 Python에서 재계산하여 2차 검증
- YOY, 비율, 파생 지표에 대해 PASS / FAIL 검증
- 오류 발생 시: 어떤 지표가 어떤 규칙에서 왜 실패했는지 로그로 기록

4) LLM 프롬프트 설계



## 3. 전체 아키텍처
<img width="975" height="516" alt="image" src="https://github.com/user-attachments/assets/e6125533-4839-4a51-85f2-22ff348f1533" />
<img width="980" height="538" alt="image" src="https://github.com/user-attachments/assets/080b6cd7-d667-4f60-9a99-a654da8e0838" />



## 4. 실행 방법
1️⃣ 환경 설정
pip install -r requirements.txt


2️⃣ 전체 실행
python run/app.py --company 삼성전자 --year 2024



## 5. 기대 효과

📉 수치 오류 없는 재무 분석

📚 근거 기반 설명 제공

🤖 LLM hallucination 최소화

🧩 확장 가능한 금융 병렬적 LLM Agent



## Quickstart (local)

### 1) Install
```bash
pip install -U duckdb sentence-transformers faiss-cpu lxml beautifulsoup4 dart-fss opendartreader
```

### 2) Environment
- `DART_API_KEY` 환경변수 설정
```bash
export DART_API_KEY="YOUR_KEY"
```

### 3) Ingest (crawl + normalize + store)
python scripts/run_ingest.py --company "LG전자" --year 2024 --seed-market --overwrite-market --qc

### 4) Calculate
python scripts/run_calc.py --company "LG전자" --year 2024 --seed-market --overwrite-market --qc

## Data layout
- DuckDB: `data/duckdb/dart.duckdb`
- Cache:  `data/cache/` (원문 xml/html, 파싱 중간 산출물)
- Workdir: 섹션별 LLM에 input으로 들어갈 metrics.json, evidence.json

## Notes
- API 키/민감 정보는 절대 코드에 하드코딩하지 말고 `.env` 또는 환경변수로 관리하세요.
- Colab 경로(`/content/drive/...`)는 repo 상대경로로 모두 치환되어 있습니다.

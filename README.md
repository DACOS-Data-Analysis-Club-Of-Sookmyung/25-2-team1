# 25-2-생성형AI 1팀
팀원 : 김소영, 이소윤, 조예린

# 📊 DART 사업보고서 기반 재무분석 AI 리포트 시스템
프로젝트 소개 PPT 를 참고해주세요 (https://www.canva.com/design/DAG-mGeuf7Y/vc07sHBVbDhlRSgnDJ1EgQ/view?utm_content=DAG-mGeuf7Y&utm_campaign=designshare&utm_medium=link2&utm_source=uniquelinks&utlId=h9631bc6555)

## 1. 프로젝트 개요
금융감독원 DART 사업보고서를 기반으로 기업의 재무 상태를 정량·정성적으로 분석하고,
근거(citation)를 포함한 **초보 투자자 대상** AI 재무 리포트를 생성하는 시스템입니다.

## 2. 핵심 기능 
📉 수치 오류 없는 재무 분석

📚 근거 기반 설명 제공

🤖 LLM hallucination 최소화

🧩 확장 가능한 섹션별 병렬 LLM Agent 구조


## 3. 전체 아키텍처
<img width="975" height="516" alt="image" src="https://github.com/user-attachments/assets/e6125533-4839-4a51-85f2-22ff348f1533" />
<img width="980" height="538" alt="image" src="https://github.com/user-attachments/assets/080b6cd7-d667-4f60-9a99-a654da8e0838" />

## 4. 폴더 구조
```bash
dart-financial-genai-report/
├─ README.md
├─ .gitignore
├─ .env
├─ requirements.txt 
├─ app.py                        # streamlit
├─ data/                         # (gitignore)
│  ├─ duckdb/
│  │  └─ dart.duckdb             
│  ├─ faiss/
│  │  └─ faiss_idmap.index(보류) # text 임베딩한 faiss
│  ├─ company_meta.csv
│  └─ cache/                     # raw html/xml, parsed intermediates
├─ resources/                    # 설정/리소스
│  ├─ company_data.py
│  ├─ corp_codes.py
├─ outputs/                      # (gitignore)
│  ├─ report.pdf                   # 최종 PDF
│  └─ sections/                  # 섹션별 JSON 산출물(디버깅/재사용) 
├─ workdir/                      # 입력묶음 + 중간 산출물 저장 (9,10장의 경우 이전 장의 summary 필요. 해당 summary 저장소)
│  ├─ meta/  
│  │  └─ meta.json               # cache_key, corp_code, bsns_year
│  ├─ metrics/                   # corp_code, bsns_year, rows[]     
│  │  ├─ s01_metrics.json  
│  │  └─ ... (섹션 추가)   
│  ├─ evidence/                  # corp_code, bsns_year, report_id, rows[]                    
│  │  ├─ s01_evidence.json  
│  │  └─ ... (섹션 추가) 
│  ├─ summary/         
│  │  ├─ bridge_summary.json        # 섹션별 요약용 (9, 10장 프롬프트 내 주입 위함)                      
├─ src/                          # core modules + entrypoints
│  │  __init__.py
│  ├─ cli.py                     # llm으로 넘어가기전까지를 담당하는 최종 run 파일 (기업이름,년도 -> ingest+calc+validate 등 모두 실행 후 최종 duckdb 저장)
│  ├─ ingest.py                  # crawl(크롤링) + normalize(정규화) + store(저장)
│  ├─ calc.py                    # calculator 
│  ├─ validate.py                # calc 검증 + db finalization
│  ├─ embed.py                   # faiss build/update (텍스트 임베딩)
│  ├─ generate.py (보류)         # section-wise LLM generation 
│  ├─ render_pdf.py              # PDF rendering
│  ├─ seed_market.py             # csv 파일에서 우리 대상인 회사와 벤티마크 분류해서 저장
│  ├─ retrieve.py (보류) 
│  ├─ utils/
│  │  ├─ __init__.py
│  │  ├─ dart.py
│  │  ├─ html.py
│  │  ├─ ids.py
│  │  ├─ normalize.py
│  │  ├─ text.py
│  └─ llm/  
│  │  ├─ __init__.py
│  │  ├─ client.py
│  └─ sections/                  # 섹션별 플러그인 
│     ├─ _common/ 
│     ├─ builders/
│     │  │  └─ create_meta.py       # meta.json 반환 
│     │  │  └─ create_metrics.py    # 각 섹션의 metrics.json 반환     
│     │  │  └─ create_evidence.py   # 각 섹션의 evidence.json 반환 
│     │  ├─ io.py           
│     │  ├─ tables_templates.py        
│     │  └─ sections_catalog.json
│     ├─ c01_intro/
│     │  ├─ s01_1_objective/
│     │  │  ├─ prompt.md
│     │  │  ├─ inputs_spec.json
│     │  │  ├─ retriever.py                 
│     │  ├─ s01_2_company_overview/
│     │  │  ├─ prompt.md
│     │  │  ├─ inputs_spec.json
│     │  │  ├─ retriever.py
│     └─ ... (섹션 추가)
├─ prompts/                      # 프롬프트 원본 모음(공통/레퍼런스)
│  ├─ _common/
│  │  ├─ system_rules.md         # 공통 규칙(숫자 생성 금지/근거 강제/JSON 출력)
│  │  └─ output_schema.json      # 공통 출력 스키마(참조용)
│  │  └─ builders/
│  │  │  └─ create_meta.py       # meta.json 반환 
│  │  │  └─ create_metrics.py    # 각 섹션의 metrics.json 반환     
│  │  │  └─ create_evidence.py   # 각 섹션의 evidence.json 반환    
├─ scripts/                      # 개발자용 빠른 실행 
│  ├─ run_ingest.py              # csv → crawl → normalize → store  
│  ├─ run_calc.py                # calc.py(fact_metrics 적재 등) → validate.py(검증 파이프) -> json 출력
│  ├─ run_embed.py (보류) 
│  ├─ run_all_local.py           # ingest → calc → embed → generate → render
│  └─ run_section.py             # 특정 section만 .json 만드는 코드
│  └─ run_seed_market.py         # 벤치마크 DB 2개 생성하는 코드
│  └─ test_one_section.py
│  ├─ build_report_pdf.py 
```

## 5. 실행 방법
1️⃣ 환경 설정
```bash
pip install -r requirements.txt
```

2️⃣ 전체 실행
```bash
python run/app.py --company 삼성전자 --year 2024
```



## Quickstart (local)

### 1) Install
```bash
pip install -U duckdb sentence-transformers faiss-cpu lxml beautifulsoup4 dart-fss opendartreader streamlit reportlab>=4.0.0
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

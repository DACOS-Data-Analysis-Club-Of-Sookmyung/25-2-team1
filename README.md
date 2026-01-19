# 25-2-team1
생성 AI 1팀

# DART Financial GenAI Report
📊 DART 기반 재무분석 AI 리포트 시스템

공시 데이터(DART)를 구조적으로 해석하고, 계산 가능한 지표는 코드로 검증하며,
주석·설명 텍스트는 근거 기반 RAG로 생성하는 금융 분석 AI 리포트 파이프라인

1. 프로젝트 개요

본 프로젝트는 금융감독원 DART 사업보고서를 기반으로
기업의 재무 상태를 정량·정성적으로 분석하고,
근거(citation)를 포함한 AI 재무 리포트를 자동 생성하는 시스템입니다.

기존 범용 LLM 기반 금융 분석의 한계인

❌ 숫자 계산 오류

❌ 근거 없는 요약(hallucination)

❌ 표·주석 정보의 비구조적 활용

을 해결하기 위해, 계산 파이프라인 + 검증 파이프라인 + RAG 구조를 결합한 설계를 채택했습니다.

2. 핵심 특징 (Key Features)
1) 정량 데이터는 “코드로 계산”

연결재무제표 표 데이터를 DuckDB에 구조적으로 저장

매출, 자산, 부채 등 raw 값

유동비율, 부채비율, ROE 등 ratio/derived 지표

SQL + Python 기반 계산 → LLM은 계산에 관여하지 않음

2) 정성 데이터는 “근거 기반 생성”

연결재무제표 주석(III-3) 텍스트를 섹션/청크 단위로 분리

질문/리포트 요청 시 관련 주석 텍스트 + 표 참조를 근거로 제공

3) 계산 결과 검증 파이프라인

SQL 계산 결과를 Python에서 재계산

YOY, 비율, 파생 지표에 대해 PASS / FAIL 검증

오류 발생 시: 어떤 지표가 어떤 규칙에서 왜 실패했는지 로그로 기록

4) 표(Table)와 텍스트(Text)의 분리 저장

표:행/열/값/숫자값/단위/주석 참조(note_refs)

DuckDB에서 정확 조회·계산

텍스트: 설명·정책·회계 기준


3. 전체 아키텍처
[사용자 요청]
 (회사, 연도, 분석 항목)
        ↓
[DART 사업보고서 수집]
        ↓
[구조적 분리]
 ├─ 재무제표 표 (III-2)
 └─ 주석 텍스트/표 (III-3)
        ↓
[저장]
 ├─ DuckDB : 표, 수치, 메타데이터
 └─ FAISS  : 주석 텍스트 임베딩
        ↓
[지표 계산 + 검증]
        ↓
[RAG 근거 검색]
        ↓
[LLM 리포트 생성]
 (근거 포함)

4. 데이터 구성
📌 사용 데이터

DART 사업보고서

III. 재무에 관한 사항

2. 연결재무제표

연결재무제표 주석

주가 데이터 (CSV)

PER, PBR, PSR, PCFR 계산용

벤치마크 기업 메타 정보

5. DB 스키마 개요 (DuckDB)

주요 테이블:

reports

report_sections

rag_text_chunks

rag_tables

rag_table_cells

fs_line_items

fact_metrics

market_data

benchmark_map

📌 정책


6. 디렉토리 구조
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
│     │  ├─ io.py           
│     │  ├─ tables_templates.py        
│     │  └─ sections_catalog.json
│     ├─ c01_intro/
│     │  ├─ s01_intro/
│     │  │  ├─ prompt.md
│     │  │  ├─ inputs_spec.json
│     │  │  ├─ retriever.py                 
│     │  └─ └─ postprocess.py
│     ├─ c01_intro/
│     │  ├─ s02_bs_assets/
│     │  │  ├─ prompt.md
│     │  │  ├─ inputs_spec.json
│     │  │  ├─ retriever.py
│     │  │  └─ postprocess.py
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


7. 실행 방법
1️⃣ 환경 설정
```bash
pip install -r requirements.txt
```

2️⃣ 전체 실행
```bash
python run/app.py --company 삼성전자 --year 2024
```

9. 기대 효과

📉 수치 오류 없는 재무 분석

📚 근거 기반 설명 제공

🤖 LLM hallucination 최소화

🧩 확장 가능한 금융 RAG 구조

10. 향후 확장

뉴스/공시 비교 분석

연도별 추이 자동 분석

기업 간 벤치마크 리포트

투자자 성향별 리포트 생성

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

### 4) Embed (build/update FAISS from DuckDB chunks)
```bash
python -m src.cli embed --model "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
```

## Data layout
- DuckDB: `data/duckdb/dart.duckdb`
- FAISS:  `data/faiss/index.faiss` + `data/faiss/meta.json`
- Cache:  `data/cache/` (원문 xml/html, 파싱 중간 산출물)

## Notes
- API 키/민감 정보는 절대 코드에 하드코딩하지 말고 `.env` 또는 환경변수로 관리하세요.
- Colab 경로(`/content/drive/...`)는 repo 상대경로로 모두 치환되어 있습니다.

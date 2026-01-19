# 25-2-team1
생성 AI 1팀

# DART Financial GenAI Report


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

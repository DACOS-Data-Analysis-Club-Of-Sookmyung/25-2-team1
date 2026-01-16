[ROLE]
너는 초보 투자자를 위한 “수익성 구조 종합 해석”을 작성한다.
효율성(회전율) → 수익성(ROA/ROE/ROC) → ROE 구조(듀퐁 3요인) → 시장평가(PER/PBR/PSR)를
하나의 흐름으로 연결하여,
이 기업의 수익성이 어떤 구조로 형성되고 있는지를 설명한다.

[INPUT]

- 기업: {corp_name}
- 기준연도: {bsns_year}

- 효율성 요약: {turnover_ratio_table}
- 수익성 요약: {profitability_ratio_table}
- ROE 구조 요약(듀퐁 3요인): {dupont_3factor_table}
- 시장가치 요약: {market_ratio_table}

- (선택) 벤치마크 요약:
  {bench_turnover_table}
  {bench_profitability_table}
  {bench_dupont_table}
  {bench_market_ratio_table}

- 핵심 근거 chunk: {key_evidence_ch7}

[VERY IMPORTANT RULES]

- 어떤 지표도 새로 계산하지 않는다.
- 7.1~7.4 내용을 반복 설명하지 말고 “연결·요약”에 집중한다.
- 전기 대비(추세)와 벤치마크 대비(수준)를 문단으로 분리해 서술한다.
- 원인 단정은 {key_evidence_ch7} 근거가 있을 때만 한다.
  - 없으면 “근거가 제한적”이라고 명시한다.
- 벤치마크 표가 “제공 없음”인 항목은 “비교 제한적”이라고 명시한다.
- 말미에 참고한 주석(note_no) 목록을 반드시 포함한다.

[OUTPUT FORMAT]
A) 한눈에 보는 결론
B) 전기 대비 종합 해석(추세)
C) 벤치마크 대비 종합 해석(수준)
D) 구조적 강점
E) 점검 필요 포인트
F) 초보 투자자 체크 포인트
G) 참고 주석 목록(note_no)

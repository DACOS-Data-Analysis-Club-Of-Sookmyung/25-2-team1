[ROLE]
너는 초보 투자자를 위한 “수익성 지표(ROA/ROE/ROC) 해설”을 작성한다.
같은 이익이라도 ‘자산/자본/투하자본’ 중 무엇을 기준으로 보느냐에 따라 해석이 달라진다는 점을 쉽게 설명한다.
전기 대비 변화와 벤치마크 대비 수준을 동일 비중으로 설명한다.

[INPUT]

- 기업: {corp_name}
- 기준연도: {bsns_year}

- (구성요소 표) 당기순이익/영업이익/법인세비용/총자산/자기자본 관련 요소: {profitability_components_table}
- (수익성 표) ROA/ROE/ROC(당기·전기·증감·증감률): {profitability_ratio_table}
- (선택) 벤치마크 ROA/ROE/ROC 비교: {bench_profitability_table}

- 주석 근거 텍스트 chunk: {profitability_evidence}

[VERY IMPORTANT RULES]

- 어떤 비율도 계산하지 말고 {profitability_ratio_table} 값만 사용한다.
- ROC의 정의는 입력 기준을 그대로 따른다(재정의/재계산 금지).
- 전기 대비 파트와 벤치마크 파트는 문단을 분리한다.
- 원인 단정은 {profitability_evidence} 근거가 있을 때만 한다.
  - 없으면 “근거가 제한적”이라고 명시한다.
- 근거 표기: (근거: note_no=..., section_code=..., chunk_id=...)
- 벤치마크 표가 “제공 없음”이면 비교 제한적이라고 명시한다.
- OUTPUT FORMAT A~G 포함.

[OUTPUT FORMAT]

### 7.2 ROA, ROE, ROC 분석

(A~G 원문 구조 유지)

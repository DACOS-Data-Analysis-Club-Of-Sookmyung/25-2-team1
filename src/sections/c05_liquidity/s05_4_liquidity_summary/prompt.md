[ROLE]
너는 초보 투자자를 위한 “유동성 종합 평가”를 작성한다.
유동비율·당좌비율·현금비율을 함께 놓고,
전기 대비 변화와 벤치마크 대비 수준을 동일 비중으로 종합 해석한다.

[INPUT]

- 기업: {corp_name}
- 기준연도: {bsns_year}

- 유동성 지표 요약(유동/당좌/현금비율): {liquidity_ratio_summary_table}
- 구성요소 요약(유동자산/재고/현금/유동부채): {liquidity_components_summary_table}
- 벤치마크 요약: {benchmark_liquidity_summary_table}

- 핵심 근거 chunk: {key_evidence}

[VERY IMPORTANT RULES]

- 비율 계산 금지(입력 표만 사용)
- 5.1~5.3의 설명을 반복하지 말고 “연결해서” 해석한다.
- 전기 대비(추세)와 벤치마크(수준)를 반드시 교차해서 결론을 낸다.
- 원인 단정은 {key_evidence} 근거가 있을 때만.
- 말미에 참고한 주석(note_no) 목록 포함.
- OUTPUT FORMAT A~F 모두 포함.
- 벤치마크 표가 “제공 없음”이면 비교가 제한적이라고 명시한다.

[OUTPUT FORMAT]

### 5.4 유동성에 대한 종합 평가

(A~F 원문 구조 그대로)

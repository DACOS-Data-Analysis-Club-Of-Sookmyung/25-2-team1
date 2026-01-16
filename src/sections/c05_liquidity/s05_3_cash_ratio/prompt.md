[ROLE]
너는 초보 투자자를 위한 “현금비율(가장 보수적인 유동성 지표) 해설”을 작성한다.
전기 대비 변화와 벤치마크 대비 수준을 동일 비중으로 설명한다.
원인 설명은 주석 근거가 있을 때만 단정한다.

[INPUT]

- 기업: {corp_name}
- 기준연도: {bsns_year}

- 현금및현금성자산/유동부채 표(당기/전기/증감 포함): {bs_cash_liab_table}
- 현금비율 표(당기/전기/증감 포함): {cash_ratio_table}
- (선택) 벤치마크 현금비율 비교: {bench_cash_ratio_table}

- 주석 근거 텍스트 chunk: {cash_evidence}

[VERY IMPORTANT RULES]

- 어떤 비율도 계산하지 말고 입력 표의 값만 사용한다.
- 전기 대비 분석과 벤치마크 분석을 동일 비중으로 다루되, 문단을 분리한다.
- “왜 변했는지”는 {cash_evidence} 근거 chunk가 있을 때만 단정한다.
  - 근거가 없으면 “근거가 제한적”이라고 명시한다.
- 근거 표기: (근거: note_no=..., section_code=..., chunk_id=...)
- 섹션 말미에 참고한 주석 목록(note_no)을 반드시 포함한다.
- OUTPUT FORMAT의 A~F를 모두 포함한다.
- bench_cash_ratio_table이 “제공 없음”이면 비교가 제한적이라고 명시한다.

[OUTPUT FORMAT]

### 5.3 현금비율

(A~F 원문 구조 그대로)

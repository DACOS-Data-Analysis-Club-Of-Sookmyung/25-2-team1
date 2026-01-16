[ROLE]
너는 초보 투자자를 위한 “현금보상비율(현금 기준 이자 상환능력) 해설”을 작성한다.
이 지표를 통해 이익이 아니라 ‘실제 현금(OCF)’ 기준으로
회사가 이자 부담을 감당할 여력이 있는지를 설명한다.
전기 대비 변화(추세)와 벤치마크 대비 수준(상대 위치)을 동일한 비중으로 해석한다.
원인 단정은 주석 근거가 있을 때만 수행한다.

[INPUT]

- 기업: {corp_name}
- 기준연도: {bsns_year}

- 영업활동현금흐름(OCF) 표(당기/전기/증감 포함): {cf_ocf_table}
- 현금보상비율 표(당기/전기/증감 포함): {cash_coverage_ratio_table}
- (선택) 벤치마크 현금보상비율 비교: {bench_cash_coverage_ratio_table}

- 주석 근거 텍스트 chunk: {cash_coverage_evidence}

[VERY IMPORTANT RULES]

- 비율을 새로 계산하지 않는다. (현금보상비율은 입력 표 값만 사용)
- 전기 대비 분석과 벤치마크 분석을 문단으로 분리한다.
- 원인 단정은 {cash_coverage_evidence} 근거 있을 때만.
- 근거 표기: (근거: note_no=..., section_code=..., chunk_id=...)
- 근거가 부족하면 “주석 근거가 제한적”이라고 명시한다.
- 말미에 note_no 목록 포함.
- OUTPUT FORMAT A~F 포함.
- bench_cash_coverage_ratio_table이 “제공 없음”이면 비교가 제한적이라고 명시한다.

[OUTPUT FORMAT]

### 6.4 현금보상비율

(A~F 원문 구조 동일)

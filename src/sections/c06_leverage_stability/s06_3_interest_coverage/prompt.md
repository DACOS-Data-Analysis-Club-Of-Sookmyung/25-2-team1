[ROLE]
너는 초보 투자자를 위한 “이자보상비율(이익 기반 이자 상환능력) 해설”을 작성한다.
전기 대비 변화와 벤치마크 대비 수준을 동일 비중으로 설명한다.
원인 설명은 주석 근거가 있을 때만 단정한다.

[INPUT]

- 기업: {corp_name}
- 기준연도: {bsns_year}

- (참고) 영업이익 구성요소 표: {is_op_interest_components_table}
- 이자보상비율 표: {interest_coverage_ratio_table}
- (선택) 벤치마크 이자보상비율 비교: {bench_interest_coverage_ratio_table}

- 주석 근거 텍스트 chunk: {interest_evidence}

[VERY IMPORTANT RULES]

- 비율 계산 금지(입력 표 값만 사용).
- 전기 대비 파트와 벤치마크 파트를 문단으로 분리한다.
- 원인 단정은 {interest_evidence} 근거 chunk 있을 때만.
- 근거 표기: (근거: note_no=..., section_code=..., chunk_id=...)
- 말미에 note_no 목록 포함.
- OUTPUT FORMAT A~F 포함.
- 구성요소 표에 이자비용이 없으면, 이자비용 수치 해석은 생략하고 “제공되지 않음”을 명시한다.
- bench_interest_coverage_ratio_table이 “제공 없음”이면 비교가 제한적이라고 명시한다.

[OUTPUT FORMAT]

### 6.3 이자보상비율

(A~F 원문 구조 동일)

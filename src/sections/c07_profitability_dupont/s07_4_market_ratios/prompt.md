[ROLE]
너는 초보 투자자를 위한 “시장가치 지표(PER/PBR/PSR) 해설”을 작성한다.
PER/PBR/PSR을 통해 시장 가격이 기업의 이익·자본·매출을 어떻게 ‘상대적으로’ 반영하고 있는지 설명한다.
고평가/저평가를 단정하지 않고 가능한 해석 시나리오를 제시한다.
전기 대비 변화와 벤치마크 대비 수준을 동일 비중으로 설명한다.

[INPUT]

- 기업: {corp_name}
- 기준연도: {bsns_year}

- 시장가치 지표 표(PER/PBR/PSR, 당기·전기(가능시)·증감·증감률): {market_ratio_table}
- (구성요소 표) 주가/발행주식수(가능시): {market_components_table}
- (선택) 벤치마크 PER/PBR/PSR 비교: {bench_market_ratio_table}

- 주석 근거 텍스트 chunk: {market_evidence}

[VERY IMPORTANT RULES]

- 어떤 시장지표도 계산하지 않는다. 입력 표 값만 사용한다.
- “고평가/저평가” 단정 금지. “가능성”으로만 서술한다.
- 전기 대비 파트와 벤치마크 파트는 문단을 분리한다.
- 전기 값이 null이면 “전기 비교가 제한적”이라고 명시한다.
- 주식수 변동 등 구조 변화 언급은 {market_evidence} 근거가 있을 때만 단정한다.
- 근거 표기: (근거: note_no=..., section_code=..., chunk_id=...)
- 벤치마크 표가 “제공 없음”이면 비교 제한적이라고 명시한다.
- 섹션 말미에 참고 주석(note_no) 목록 포함.
- OUTPUT FORMAT A~G 포함.

[OUTPUT FORMAT]

### 7.4 PER, PBR, PSR 분석

(A~G 원문 구조 유지, F는 evidence 기반으로 투명성 작성)

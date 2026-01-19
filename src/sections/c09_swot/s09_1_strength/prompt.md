[ROLE]
너는 기업의 재무 성과와 경쟁 환경을 종합하여,
전략적으로 의미 있는 강점을 선별하는 애널리스트이다.

[INPUT]

- 기업명: {corp_name}
- 분석 연도: {bsns_year}

- 1~8장 핵심 요약(Bridge):
  {bridge_text}

- 핵심 Strength 후보(AND 기준):
  {strength_core_table}

- 보조 Strength 후보(OR 기준):
  {strength_aux_table}

[TASK]
다음 기준에 따라 Strength를 도출하라.

[DECISION RULE]

1. AND 기준(전기 대비 긍정 AND 벤치마크 대비 긍정)만 핵심 Strength로 선정한다.
2. OR 기준(둘 중 하나만 긍정)은 핵심 Strength로 분류하지 말고 보조 Strength로만 언급한다.
3. 표는 그대로 출력하지 말고, 표의 내용을 근거로 “의미 중심”으로 설명한다.
4. 새 숫자 생성 금지. 표에 있는 값만 사용한다.

[OUTPUT STRUCTURE]

1. Strength 요약
2. 핵심 Strength (AND 기준)
   - 전기 대비 흐름 설명
   - 벤치마크 대비 상대적 우위 설명
3. 보조 Strength (OR 기준, 선택)
   - 전기 대비 또는 벤치마크 대비에서만 긍정적인 요소를 간략히 언급

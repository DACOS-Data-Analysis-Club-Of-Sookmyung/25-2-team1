from google.colab import drive
import duckdb
import pandas as pd
import re

drive.mount('/content/drive')

DB_PATH = "/content/drive/MyDrive/DACOS/AI_Project/생성형AI/data/dart_rag.duckdb"

con = duckdb.connect(DB_PATH)
print("✅ connected:", DB_PATH)


# 1) label 정규화 함수
def norm_label(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.lower()
    # 공백/구두점/괄호류/특수기호 제거
    s = re.sub(r"[\s\.\,\-\(\)\/\[\]·•:;]+", "", s)
    return s

ACCOUNT_MAP = [
    # --- BS ---
    ("BS", "TOTAL_ASSETS", ["자산총계"]),
    ("BS", "CURRENT_ASSETS", ["유동자산"]),
    ("BS", "CASH_EQ", ["현금및현금성자산", "현금 및 현금성자산", "현금및예치금"]),
    ("BS", "AR", ["매출채권"]),
    ("BS", "INVENTORIES", ["재고자산"]),
    ("BS", "NON_CURRENT_ASSETS", ["비유동자산"]),
    ("BS", "PPE", ["유형자산"]),
    ("BS", "IA", ["무형자산"]),


    ("BS", "TOTAL_LIABILITIES", ["부채총계"]),
    ("BS", "CURRENT_LIABILITIES", ["유동부채"]),
    ("BS", "AP", ["매입채무"]),
    ("BS", "SHORT_TERM_DEBT", ["단기차입금"]),
    ("BS", "NON_CURRENT_LIABILITIES", ["비유동부채", "장기부채"]),
    ("BS", "LONG_TERM_DEBT", ["장기차입금"]),
    ("BS", "DEFERRED_TAX_LIAB", ["이연법인세부채"]),

    ("BS", "EQUITY", ["자본총계", "자기자본"]),
    ("BS", "PARENT_EQUITY", ["지배기업 소유주지분", "지배기업의 소유주에게 귀속되는 자본"]),
    ("BS", "CAPITAL_STOCK", ["자본금"]),
    ("BS", "RETAINED_EARNINGS", ["이익잉여금"]),
    ("BS", "NON_CONTROLLING_INTEREST", ["비지배지분"]),

    # --- IS ---
    ("IS_CIS", "REVENUE", ["매출액", "영업수익"]),
    ("IS_CIS", "COGS", ["매출원가"]),
    ("IS_CIS", "GROSS_PROFIT", ["매출총이익"]),

    ("IS_CIS", "OP_PROFIT", ["영업이익", "영업이익(손실)"]),
    ("IS_CIS", "INTEREST_EXP", ["이자비용", "금융비용"]),


    ("IS_CIS", "PRE_TAX_INCOME", ["법인세비용차감전순이익", "법인세비용차감전순이익(손실)"]),
    ("IS_CIS", "TAX_EXP", ["법인세비용", "법인세비용(수익)", "법인세수익(비용)"]),
    ("IS_CIS", "NET_INCOME", ["당기순이익", "당기순이익(손실)", "당기순이익(손실)(A)"]),

    ("IS_CIS", "SGA_EXPENSES", ["판매비와관리비", "판관비"]),

    # --- CF ---
    ("CF", "OCF", ["영업활동으로 인한 현금흐름","영업활동현금흐름", "영업활동으로부터의 현금흐름"]),
    ("CF", "ICF", ["투자활동으로 인한 현금흐름","투자활동현금흐름"]),
    ("CF", "FCF_FIN", ["재무활동으로 인한 현금흐름","재무활동현금흐름"]),
    ("CF", "PURCHASE_PPE", ["유형자산의 취득"]),
    ("CF", "PURCHASE_INTANGIBLES", ["무형자산의 취득"]),
    ("CF", "PURCHASE_LT_FIN_ASSETS", ["장기금융상품의 취득"]),
    ("CF", "DISPOSAL_LT_FIN_ASSETS", ["장기금융상품의 처분"]),
]

# 2) 룰 테이블로 변환 : match_type/priority 포함 (EXACT 기본)
priority_by_type = {"EXACT": 10, "REGEX": 20, "LIKE": 30}

rows = []
for scope, std_key, aliases in ACCOUNT_MAP:
    for a in aliases:
        rows.append({
            "scope": scope,
            "std_key": std_key,
            "match_type": "EXACT",
            "pattern_raw": str(a),
            "pattern": norm_label(a),
            "priority": priority_by_type["EXACT"],
            "min_indent": None,
            "max_indent": None,
        })

dedup = {}
for r in rows:
    k = (r["scope"], r["std_key"], r["match_type"], r["pattern"])
    # 같은 PK가 여러 번 들어오면 첫 번째만 유지 (원하면 priority 더 작은 걸로 교체 가능)
    if k not in dedup:
        dedup[k] = r

rows_dedup = list(dedup.values())
print("rows:", len(rows), "rows_dedup:", len(rows_dedup))

con.execute("""
CREATE TABLE IF NOT EXISTS account_map_rules (
  scope VARCHAR, -- 'BS' / 'IS_CIS' / 'CF'
  std_key VARCHAR,
  match_type VARCHAR, -- 'EXACT' (추후 REGEX/LIKE 확장)
  pattern VARCHAR, -- norm_label된 값
  pattern_raw VARCHAR,
  priority INTEGER,
  min_indent INTEGER,
  max_indent INTEGER,
  is_active BOOLEAN DEFAULT TRUE,
  note VARCHAR,
  PRIMARY KEY (scope, std_key, match_type, pattern)
);
""")

# 전체 리로드(안전): 기존 전부 비우고 다시 적재
con.execute("DELETE FROM account_map_rules;")

con.executemany("""
INSERT INTO account_map_rules
(scope, std_key, match_type, pattern, pattern_raw, priority, min_indent, max_indent, is_active, note)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE, NULL)
""", [
    (
      r["scope"], r["std_key"], r["match_type"],
      r["pattern"], r["pattern_raw"],
      r["priority"], r["min_indent"], r["max_indent"]
    )
    for r in rows_dedup
])

print("✅ account_map_rules loaded:", con.execute("SELECT COUNT(*) FROM account_map_rules").fetchone()[0])

# ============================================================
# 1) v_fin_long_raw
#    - fs_facts에는 corp_code/bsns_year가 없으므로 반드시 reports JOIN
# ============================================================

con.execute("DROP VIEW IF EXISTS v_fin_long_raw;")
con.execute(r"""
CREATE VIEW v_fin_long_raw AS
SELECT
  rp.corp_code,
  rp.bsns_year,
  f.report_id,

  li.statement_type,
  li.ifrs_code,
  li.label_clean,

  -- label_norm: account_map의 pattern과 동일 규칙으로 정규화
  regexp_replace(
    lower(coalesce(li.label_clean,'')),
    '[\s\.\,\-\(\)\/\[\]·•:;]+',
    '',
    'g'
  ) AS label_norm,

  f.period_end,
  f.fiscal_year,
  f.value,
  f.unit_multiplier,
  f.currency,
  (f.value * f.unit_multiplier) AS value_won,

  f.note_refs_raw,
  f.line_item_id,
  f.table_id,
  f.row_idx,
  f.col_idx
FROM fs_facts f
JOIN fs_line_items li
  ON li.line_item_id = f.line_item_id
JOIN reports rp
  ON rp.report_id = f.report_id
WHERE f.value IS NOT NULL
  AND f.unit_multiplier IS NOT NULL;
""")
print("✅ v_fin_long_raw created")

# ----------------------------
# v_fin_long_mapped (FINAL)
# - account_map_rules only
# - no LATERAL
# - indent_level via rag_table_rows (table_id + row_idx)
# - label_norm computed in SQL
# ----------------------------

con.execute("DROP VIEW IF EXISTS v_fin_long_mapped;")
con.execute(r"""
CREATE VIEW v_fin_long_mapped AS
WITH base AS (
  SELECT
    f.*,

    -- ✅ label_norm: SQL에서 정규화 (EXACT 매칭용)
    lower(regexp_replace(f.label_clean, '[\s\.\,\-\(\)\/\[\]·•:;]+', '', 'g')) AS label_norm,

    -- ✅ indent_level SSOT: rag_table_rows에서 정확히 가져오기
    rtr.indent_level AS indent
  FROM v_fin_long_raw f
  LEFT JOIN rag_table_rows rtr
    ON rtr.table_id = f.table_id
   AND rtr.row_idx  = f.row_idx
  WHERE f.label_clean IS NOT NULL
),
matched AS (
  SELECT
    b.*,
    r.std_key,
    r.priority,
    r.pattern_raw AS matched_pattern_raw,

    ROW_NUMBER() OVER (
      PARTITION BY b.corp_code, b.bsns_year, b.report_id, b.statement_type, b.fiscal_year, b.table_id, b.row_idx
      ORDER BY
        CASE WHEN r.priority IS NOT NULL THEN 0 ELSE 1 END ASC,
        r.priority ASC NULLS LAST
    ) AS rn
  FROM base b
  LEFT JOIN account_map_rules r
    ON r.is_active = TRUE
   AND r.scope = b.statement_type
   AND r.match_type = 'EXACT'
   AND r.pattern = b.label_norm
)
SELECT
  -- report grain
  corp_code,
  bsns_year,
  report_id,

  -- source identifiers (디버깅/추적용)
  table_id,
  row_idx,
  statement_type,
  fiscal_year,
  line_item_id,

  -- label & structure
  label_clean,
  label_norm,
  indent,

  -- value
  value_won,
  unit_multiplier,
  note_refs_raw,

  -- mapping result
  std_key,
  matched_pattern_raw,
  priority

FROM matched
WHERE rn = 1
;
""")

print("✅ v_fin_long_mapped recreated (unit -> unit_multiplier)")

# ============================================================
# 3) v_summary_std
#    - 요약은 "매핑된 항목만" (매핑 실패는 자연스럽게 빠짐)
#    - corp_code/bsns_year/std_key 기준 집계
# ============================================================

con.execute("DROP VIEW IF EXISTS v_summary_std;")
con.execute(r"""
CREATE VIEW v_summary_std AS
WITH base AS (
  SELECT *
  FROM v_fin_long_mapped
  WHERE std_key IS NOT NULL
    AND fiscal_year = bsns_year   -- 동일 회계연도만(필요하면 제거 가능)
),
picked AS (
  SELECT
    corp_code,
    bsns_year,
    statement_type,
    std_key,
    max_by(value_won, abs(value_won)) AS th
  FROM base
  GROUP BY corp_code, bsns_year, statement_type, std_key
)
SELECT
  corp_code,
  bsns_year,
  statement_type,
  std_key,
  th
FROM picked;
""")
print("✅ v_summary_std created")

# ============================================================
# 3-1) v_summary_all_years (증감 분석을 위한 베이스)
# - bsns_year 필터를 제거하여 모든 가용 연도 데이터를 포함
# ============================================================
con.execute("DROP VIEW IF EXISTS v_summary_all_years;")
con.execute(r"""
CREATE VIEW v_summary_all_years AS
WITH picked AS (
    SELECT
        corp_code,
        fiscal_year,
        statement_type,
        std_key,
        max_by(value_won, abs(value_won)) AS val
    FROM v_fin_long_mapped
    WHERE std_key IS NOT NULL
    GROUP BY 1, 2, 3, 4
)
SELECT * FROM picked;
""")

# ============================================================
# 3-2) v_analysis_compare (전기 대비 증감 계산)
# - 당기(T)와 전기(T-1)를 조인하여 증감액/증감률 산출
# ============================================================
con.execute("DROP VIEW IF EXISTS v_analysis_compare;")
con.execute(r"""
CREATE VIEW v_analysis_compare AS
SELECT
    curr.corp_code,
    curr.fiscal_year AS bsns_year,
    curr.statement_type,
    curr.std_key,
    curr.val AS val_curr,
    prev.val AS val_prev,
    (curr.val - COALESCE(prev.val, 0)) AS diff_amt,
    CASE
        WHEN prev.val IS NOT NULL AND prev.val != 0
        THEN (curr.val - prev.val) / abs(prev.val) * 100
        ELSE NULL
    END AS diff_rate
FROM v_summary_all_years curr
LEFT JOIN v_summary_all_years prev
    ON curr.corp_code = prev.corp_code
   AND curr.fiscal_year = prev.fiscal_year + 1
   AND curr.std_key = prev.std_key
   AND curr.statement_type = prev.statement_type;
""")

# ============================================================
# 4) 분석용 데이터 추출 함수 (예시: 자산 구조 분석용)
# ============================================================
def get_analysis_tables(con, corp_code, bsns_year):
    # (1) 자산 항목 표 in BS (당기/전기/증감액/증감률)
    asset_keys = [
        'TOTAL_ASSETS', 'CURRENT_ASSETS','CASH_EQ', 'AR',
        'INVENTORIES', 'NON_CURRENT_ASSETS', 'PPE', 'IA'
    ]

    bs_assets_table = con.execute("""
        SELECT
            std_key AS 항목,
            val_curr AS 당기,
            val_prev AS 전기,
            diff_amt AS 증감액,
            diff_rate AS 증감률
        FROM v_analysis_compare
        WHERE corp_code = ? AND bsns_year = ?
          AND std_key IN ({})
        ORDER BY CASE std_key
            WHEN 'TOTAL_ASSETS' THEN 1 WHEN 'CURRENT_ASSETS' THEN 2
            WHEN 'NON_CURRENT_ASSETS' THEN 3 WHEN 'CASH_EQ' THEN 4
            WHEN 'AR' THEN 5 WHEN 'INVENTORIES' THEN 6
            WHEN 'PPE' THEN 7 END
    """.format(", ".join([f"'{k}'" for k in asset_keys])), [corp_code, bsns_year]).df()

    return bs_assets_table

# ============================================================
# 4) v_wide_core
#    - "계산용 wide"의 최소 코어
#    - 매핑 덜 되어도 NULL로 남기고 다음 단계에서 해결(v_value_resolved)
# ============================================================

con.execute("DROP VIEW IF EXISTS v_wide_core;")
con.execute(r"""
CREATE VIEW v_wide_core AS
WITH base AS (
  SELECT *
  FROM v_fin_long_mapped
  WHERE std_key IS NOT NULL
    AND fiscal_year = bsns_year
),
picked AS (
  SELECT
    corp_code, bsns_year,
    std_key,
    max_by(value_won, abs(value_won)) AS v
  FROM base
  GROUP BY corp_code, bsns_year, std_key
)
SELECT
  corp_code,
  bsns_year,

  -- BS core
  max(v) FILTER (WHERE std_key='TOTAL_ASSETS')            AS total_assets,
  max(v) FILTER (WHERE std_key='CURRENT_ASSETS')          AS current_assets,
  max(v) FILTER (WHERE std_key='INVENTORIES')             AS inventories,
  max(v) FILTER (WHERE std_key='CASH_EQ')                 AS cash_eq,
  max(v) FILTER (WHERE std_key='AR')                      AS ar,

  max(v) FILTER (WHERE std_key='TOTAL_LIABILITIES')       AS total_liabilities,
  max(v) FILTER (WHERE std_key='CURRENT_LIABILITIES')     AS current_liabilities,
  max(v) FILTER (WHERE std_key='NON_CURRENT_LIABILITIES') AS non_current_liabilities,
  max(v) FILTER (WHERE std_key='LONG_TERM_DEBT')          AS long_term_debt,
  max(v) FILTER (WHERE std_key='SHORT_TERM_DEBT')         AS short_term_debt,

  max(v) FILTER (WHERE std_key='EQUITY')                  AS equity,

  -- IS/CIS core
  max(v) FILTER (WHERE std_key='REVENUE')                 AS revenue,
  max(v) FILTER (WHERE std_key='COGS')                    AS cogs,
  max(v) FILTER (WHERE std_key='OP_PROFIT')               AS op_profit,
  max(v) FILTER (WHERE std_key='NET_INCOME')              AS net_income,
  max(v) FILTER (WHERE std_key='TAX_EXP')                 AS tax_exp,
  max(v) FILTER (WHERE std_key='INTEREST_EXP')            AS interest_exp,

  -- CF core
  max(v) FILTER (WHERE std_key='OCF')                     AS ocf

FROM picked
GROUP BY corp_code, bsns_year;
""")
print("✅ v_wide_core created")

# ============================================================
# 5) ratio_requirements (테이블)
# ============================================================

con.execute("""
CREATE TABLE IF NOT EXISTS ratio_requirements (
  ratio_key VARCHAR,          -- 예: current_ratio
  ratio_ko  VARCHAR,          -- 예: 유동비율
  item_key  VARCHAR,          -- 필요한 std_key (예: CURRENT_ASSETS)
  role      VARCHAR,          -- numerator/denominator/extra
  required  BOOLEAN,          -- 필수 여부
  note      VARCHAR,
  PRIMARY KEY (ratio_key, item_key, role)
);
""")

con.execute("DELETE FROM ratio_requirements;")

con.execute("""
INSERT INTO ratio_requirements (ratio_key, ratio_ko, item_key, role, required, note) VALUES
-- 1) 유동성
('current_ratio', '유동비율', 'CURRENT_ASSETS', 'numerator',  TRUE,  '유동자산/유동부채'),
('current_ratio', '유동비율', 'CURRENT_LIABILITIES', 'denominator', TRUE, NULL),

('quick_ratio',   '당좌비율', 'CURRENT_ASSETS', 'numerator',  TRUE,  '(유동자산-재고자산)/유동부채'),
('quick_ratio',   '당좌비율', 'INVENTORIES',    'subtract',   FALSE, '유동자산에서 차감'),
('quick_ratio',   '당좌비율', 'CURRENT_LIABILITIES', 'denominator', TRUE, NULL),

('cash_ratio',    '현금비율', 'CASH_EQ', 'numerator', TRUE, '현금및현금성자산/유동부채'),
('cash_ratio',    '현금비율', 'CURRENT_LIABILITIES', 'denominator', TRUE, NULL),

-- 2) 부채성
('long_term_debt_ratio', '장기부채비율', 'NON_CURRENT_LIABILITIES', 'numerator', TRUE, '장기부채/총자산'),
('long_term_debt_ratio', '장기부채비율', 'TOTAL_ASSETS', 'denominator', TRUE, NULL),

('total_debt_ratio', '총부채비율', 'TOTAL_LIABILITIES', 'numerator', TRUE, '총부채/총자산'),
('total_debt_ratio', '총부채비율', 'TOTAL_ASSETS', 'denominator', TRUE, NULL),

('interest_coverage', '이자보상비율', 'OP_PROFIT', 'numerator', TRUE, '영업이익/이자비용'),
('interest_coverage', '이자보상비율', 'INTEREST_EXP', 'denominator', TRUE, NULL),

('cash_coverage_ocf', '현금보상비율(현금흐름)', 'OCF', 'numerator', TRUE, '영업현금흐름/이자비용'),
('cash_coverage_ocf', '현금보상비율(현금흐름)', 'INTEREST_EXP', 'denominator', TRUE, NULL),

('cash_coverage_op_dep', '현금보상비율(영업이익+감가상각)', 'OP_PROFIT', 'numerator', TRUE, '(영업이익+감가상각비)/이자비용'),
('cash_coverage_op_dep', '현금보상비율(영업이익+감가상각)', 'DEPRECIATION', 'add', FALSE, '영업이익에 더함'),
('cash_coverage_op_dep', '현금보상비율(영업이익+감가상각)', 'INTEREST_EXP', 'denominator', TRUE, NULL),

-- 3) 효율성
('asset_turnover', '자산 회전율', 'REVENUE', 'numerator', TRUE, '매출/총자산'),
('asset_turnover', '자산 회전율', 'TOTAL_ASSETS', 'denominator', TRUE, NULL),

('inventory_turnover', '재고자산 회전율', 'COGS', 'numerator', TRUE, '매출원가/재고자산'),
('inventory_turnover', '재고자산 회전율', 'INVENTORIES', 'denominator', TRUE, NULL),

('ar_turnover', '매출채권 회전율', 'REVENUE', 'numerator', TRUE, '매출/매출채권'),
('ar_turnover', '매출채권 회전율', 'AR', 'denominator', TRUE, NULL),

-- 4) 수익성
('roe', 'ROE', 'NET_INCOME', 'numerator', TRUE, '순이익/자기자본'),
('roe', 'ROE', 'EQUITY', 'denominator', TRUE, NULL),

('roa', 'ROA', 'NET_INCOME', 'numerator', TRUE, '순이익/총자산'),
('roa', 'ROA', 'TOTAL_ASSETS', 'denominator', TRUE, NULL),

('roc', 'ROC', 'NOPAT', 'numerator', TRUE, '세후영업이익/(장기부채+자기자본)'),
('roc', 'ROC', 'INVESTED_CAPITAL', 'denominator', TRUE, NULL),

-- 5) 시장가치
('per',  'PER',  'STOCK_PRICE',  'numerator', TRUE,  '주가/주당순이익'),
('per',  'PER',  'EPS',          'denominator', TRUE,  NULL),

('pbr',  'PBR',  'STOCK_PRICE',  'numerator', TRUE,  '주가/주당자기자본'),
('pbr',  'PBR',  'BPS',          'denominator', TRUE,  NULL),

('psr',  'PSR',  'STOCK_PRICE',  'numerator', TRUE,  '주가/주당매출'),
('psr',  'PSR',  'SPS',          'denominator', TRUE,  NULL),

('pcfr', 'PCFR', 'STOCK_PRICE',  'numerator', TRUE,  '주가/주당(세후 순이익+감가상각비)'),
('pcfr', 'PCFR', 'CFPS',         'denominator', TRUE,  NULL),

--- 6) 추가 지표

-- 순이익률 = 당기순이익 / 매출액
('net_margin', '순이익률', 'NET_INCOME', 'numerator',   TRUE, '당기순이익/매출액'),
('net_margin', '순이익률', 'REVENUE',    'denominator', TRUE, NULL),

-- 재무레버리지 = 자산총계 / 자본총계
('fin_leverage', '재무레버리지', 'TOTAL_ASSETS', 'numerator',   TRUE, '자산총계/자본총계'),
('fin_leverage', '재무레버리지', 'EQUITY',       'denominator', TRUE, NULL)
;
""")

# ============================================================
# 6) v_value_resolved
#    - "필요한 값을 어떻게든 찾기" 단계의 SSOT 뷰
#    - 1차: 표에서 매핑된 값 (v_fin_long_mapped)
#    - 2차: note_links로 연결된 주석 텍스트를 evidence로 붙여서 LLM이 보강할 수 있게 함
#
#    ✅ 중요: 여기서는 LLM을 돌리지 않음
#            대신 "값 후보 + 근거 텍스트"를 한 곳에 모아둠
# ============================================================

con.execute("DROP VIEW IF EXISTS v_value_resolved;")
con.execute("DROP VIEW IF EXISTS v_value_resolved;")
con.execute(r"""
CREATE VIEW v_value_resolved AS
WITH req AS (
  SELECT DISTINCT item_key AS std_key
  FROM ratio_requirements

  UNION ALL SELECT 'PRE_TAX_INCOME'
  UNION ALL SELECT 'TAX_EXP'
  UNION ALL SELECT 'DEPRECIATION'
  UNION ALL SELECT 'SHARES_OUTSTANDING'
  UNION ALL SELECT 'STOCK_PRICE'

),
std_scope AS (
  -- std_key는 어떤 statement_type에서만 인정할지 (ACCOUNT_MAP 기반)
  SELECT std_key, any_value(scope) AS scope
  FROM account_map_rules
  GROUP BY std_key
),
base_reports AS (
  SELECT DISTINCT corp_code, bsns_year, report_id
  FROM v_fin_long_mapped
),

-- ✅ “정확한 값 후보”만 남김: (scope 일치) AND (label EXACT 일치)
candidates AS (
  SELECT
    m.corp_code,
    m.bsns_year,
    m.report_id,
    m.std_key,
    m.value_won,
    m.label_clean,
    m.note_refs_raw
  FROM v_fin_long_mapped m
  JOIN std_scope s
    ON s.std_key = m.std_key
   AND s.scope   = m.statement_type     -- ✅ scope 강제
  JOIN account_map_rules r
    ON r.scope   = m.statement_type
   AND r.std_key = m.std_key
   AND r.match_type = 'EXACT'
   AND r.pattern = m.label_norm         -- ✅ EXACT 강제
  WHERE m.std_key IS NOT NULL
    AND m.fiscal_year = m.bsns_year
),

-- ✅ 유일하면 확정, 여러 개면 NULL (임의선택 금지)
base_vals AS (
  SELECT
    corp_code,
    bsns_year,
    report_id,
    std_key,
    CASE
      WHEN COUNT(DISTINCT value_won) = 1 THEN MAX(value_won)
      ELSE NULL
    END AS value_won,
    list(distinct label_clean) AS labels,
    list(distinct note_refs_raw) AS note_refs,
    COUNT(*) AS cand_rows,
    COUNT(DISTINCT value_won) AS cand_distinct_values
  FROM candidates
  GROUP BY corp_code, bsns_year, report_id, std_key
),

notes_evidence AS (
  SELECT
    rp.corp_code,
    rp.bsns_year,
    nl.report_id,
    nl.line_item_id,
    string_agg(tc.text, '\n\n') AS note_text
  FROM note_links nl
  JOIN reports rp ON rp.report_id = nl.report_id
  LEFT JOIN report_sections rs ON rs.section_id = nl.note_section_id
  LEFT JOIN rag_text_chunks tc
    ON tc.section_id = rs.section_id
   AND tc.section_type = 'notes'
  GROUP BY rp.corp_code, rp.bsns_year, nl.report_id, nl.line_item_id
),

note_by_stdkey AS (
  SELECT
    m.corp_code,
    m.bsns_year,
    m.report_id,
    m.std_key,
    string_agg(ne.note_text, '\n\n') AS note_text_all
  FROM v_fin_long_mapped m
  JOIN notes_evidence ne
    ON ne.report_id = m.report_id
   AND ne.line_item_id = m.line_item_id
  WHERE m.std_key IS NOT NULL
  GROUP BY m.corp_code, m.bsns_year, m.report_id, m.std_key
)

SELECT
  br.corp_code,
  br.bsns_year,
  br.report_id,
  r.std_key,
  bv.value_won,
  bv.labels,
  bv.note_refs,
  nb.note_text_all AS note_text,

  -- ✅ 디버깅용(원하면 나중에 제거)
  bv.cand_rows,
  bv.cand_distinct_values

FROM base_reports br
CROSS JOIN req r
LEFT JOIN base_vals bv
  ON bv.corp_code = br.corp_code
 AND bv.bsns_year = br.bsns_year
 AND bv.report_id = br.report_id
 AND bv.std_key = r.std_key
LEFT JOIN note_by_stdkey nb
  ON nb.corp_code = br.corp_code
 AND nb.bsns_year = br.bsns_year
 AND nb.report_id = br.report_id
 AND nb.std_key = r.std_key
;
""")
print("✅ v_value_resolved updated (EXACT + scope enforced, no arbitrary pick)")

# 복합식: 파생 키 생성하기
con.execute("DROP VIEW IF EXISTS v_value_augmented;")
con.execute(r"""
CREATE VIEW v_value_augmented AS
WITH base AS (
  -- v_value_resolved는 (corp_code, bsns_year, report_id, std_key) 단위로 이미 값이 정리돼 있다고 가정
  SELECT
    corp_code,
    bsns_year,
    report_id,

    MAX(value_won) FILTER (WHERE std_key='OP_PROFIT')              AS op_profit,
    MAX(value_won) FILTER (WHERE std_key='TAX_EXP')                AS tax_exp,
    MAX(value_won) FILTER (WHERE std_key='PRE_TAX_INCOME')         AS pre_tax_income,

    MAX(value_won) FILTER (WHERE std_key='LONG_TERM_DEBT')         AS long_term_debt,
    MAX(value_won) FILTER (WHERE std_key='NON_CURRENT_LIABILITIES') AS non_current_liabilities,
    MAX(value_won) FILTER (WHERE std_key='TOTAL_LIABILITIES')      AS total_liabilities,
    MAX(value_won) FILTER (WHERE std_key='CURRENT_LIABILITIES')    AS current_liabilities,

    MAX(value_won) FILTER (WHERE std_key='EQUITY')                 AS equity,
    MAX(value_won) FILTER (WHERE std_key='TOTAL_ASSETS')           AS total_assets,

    MAX(value_won) FILTER (WHERE std_key='NET_INCOME')             AS net_income,
    MAX(value_won) FILTER (WHERE std_key='REVENUE')                AS revenue,
    MAX(value_won) FILTER (WHERE std_key='DEPRECIATION')           AS depreciation,

    MAX(value_won) FILTER (WHERE std_key='STOCK_PRICE')            AS stock_price,
    MAX(value_won) FILTER (WHERE std_key='SHARES_OUTSTANDING')     AS shares_outstanding

  FROM v_value_resolved
  GROUP BY corp_code, bsns_year, report_id
),
derived AS (
  SELECT
    *,
    -- 장기부채 fallback
    COALESCE(
      long_term_debt,
      non_current_liabilities,
      CASE
        WHEN total_liabilities IS NOT NULL AND current_liabilities IS NOT NULL
        THEN total_liabilities - current_liabilities
        ELSE NULL
      END
    ) AS long_term_debt_resolved,

    -- tax_rate = clamp(tax_exp / pre_tax_income, 0, 1)
    CASE
      WHEN tax_exp IS NULL OR pre_tax_income IS NULL OR pre_tax_income = 0 THEN NULL
      ELSE
        CASE
          WHEN (tax_exp / pre_tax_income) < 0 THEN 0
          WHEN (tax_exp / pre_tax_income) > 1 THEN 1
          ELSE (tax_exp / pre_tax_income)
        END
    END AS tax_rate,

    -- NOPAT = OP_PROFIT * (1-tax_rate)
    CASE
      WHEN op_profit IS NULL THEN NULL
      WHEN (tax_exp IS NULL OR pre_tax_income IS NULL OR pre_tax_income = 0) THEN NULL
      ELSE op_profit * (1 - (
        CASE
          WHEN (tax_exp / pre_tax_income) < 0 THEN 0
          WHEN (tax_exp / pre_tax_income) > 1 THEN 1
          ELSE (tax_exp / pre_tax_income)
        END
      ))
    END AS nopat,

    -- 투자자금 = 장기부채(대체) + 자기자본
    CASE
      WHEN equity IS NULL THEN NULL
      ELSE COALESCE(
        long_term_debt,
        non_current_liabilities,
        CASE
          WHEN total_liabilities IS NOT NULL AND current_liabilities IS NOT NULL
          THEN total_liabilities - current_liabilities
          ELSE NULL
        END
      ) + equity
    END AS invested_capital,

    -- EPS/BPS/SPS/CFPS (분모 0 방지)
    CASE WHEN net_income IS NOT NULL AND shares_outstanding IS NOT NULL AND shares_outstanding <> 0
         THEN net_income / shares_outstanding ELSE NULL END AS eps,

    CASE WHEN equity IS NOT NULL AND shares_outstanding IS NOT NULL AND shares_outstanding <> 0
         THEN equity / shares_outstanding ELSE NULL END AS bps,

    CASE WHEN revenue IS NOT NULL AND shares_outstanding IS NOT NULL AND shares_outstanding <> 0
         THEN revenue / shares_outstanding ELSE NULL END AS sps,

    CASE WHEN net_income IS NOT NULL AND shares_outstanding IS NOT NULL AND shares_outstanding <> 0
         THEN (net_income + COALESCE(depreciation,0)) / shares_outstanding ELSE NULL END AS cfps

  FROM base
),
-- 1) 원천 값은 그대로 내려주고
raw AS (
  SELECT
    corp_code, bsns_year, report_id,
    std_key,
    value_won,
    labels,
    note_refs,
    note_text
  FROM v_value_resolved
),
-- 2) 파생값을 std_key/value_won 형태로 “추가 row”로 만든다
derived_rows AS (
  SELECT corp_code, bsns_year, report_id, 'LONG_TERM_DEBT' AS std_key, long_term_debt_resolved AS value_won FROM derived
  UNION ALL SELECT corp_code, bsns_year, report_id, 'TAX_RATE' AS std_key, tax_rate AS value_won FROM derived
  UNION ALL SELECT corp_code, bsns_year, report_id, 'NOPAT' AS std_key, nopat AS value_won FROM derived
  UNION ALL SELECT corp_code, bsns_year, report_id, 'INVESTED_CAPITAL' AS std_key, invested_capital AS value_won FROM derived
  UNION ALL SELECT corp_code, bsns_year, report_id, 'EPS' AS std_key, eps AS value_won FROM derived
  UNION ALL SELECT corp_code, bsns_year, report_id, 'BPS' AS std_key, bps AS value_won FROM derived
  UNION ALL SELECT corp_code, bsns_year, report_id, 'SPS' AS std_key, sps AS value_won FROM derived
  UNION ALL SELECT corp_code, bsns_year, report_id, 'CFPS' AS std_key, cfps AS value_won FROM derived
)
SELECT
  r.corp_code,
  r.bsns_year,
  r.report_id,
  r.std_key,
  r.value_won,
  r.labels,
  r.note_refs,
  r.note_text
FROM raw r

UNION ALL
SELECT
  d.corp_code,
  d.bsns_year,
  d.report_id,
  d.std_key,
  d.value_won,
  NULL AS labels,
  NULL AS note_refs,
  NULL AS note_text
FROM derived_rows d;
""")

print("✅ v_value_augmented created")

# ============================================================
# 7) v_financial_ratios
#    - ratio_requirements를 기준으로 필요한 값이 모두 있을 때만 계산
#    - 값이 없으면 ratio는 NULL (대신 missing 정보는 남김)
# ============================================================

con.execute("DROP VIEW IF EXISTS v_financial_ratios;")
con.execute(r"""
CREATE VIEW v_financial_ratios AS
WITH req AS (
  SELECT ratio_key, ratio_ko, item_key, role, required
  FROM ratio_requirements
),
base AS (
  SELECT DISTINCT corp_code, bsns_year, report_id
  FROM v_value_augmented
),
grid AS (
  SELECT
    b.corp_code,
    b.bsns_year,
    b.report_id,
    r.ratio_key,
    r.ratio_ko,
    r.item_key,
    r.role,
    r.required,
    v.value_won
  FROM base b
  JOIN req r ON 1=1
  LEFT JOIN v_value_augmented v
    ON v.corp_code = b.corp_code
   AND v.bsns_year = b.bsns_year
   AND v.report_id = b.report_id
   AND v.std_key = r.item_key
),
agg AS (
  SELECT
    corp_code,
    bsns_year,
    report_id,
    ratio_key,
    any_value(ratio_ko) AS ratio_ko,

    COALESCE(SUM(CASE WHEN role='numerator' THEN value_won END), 0)
  + COALESCE(SUM(CASE WHEN role='add'       THEN value_won END), 0)
  - COALESCE(SUM(CASE WHEN role='subtract'  THEN value_won END), 0) AS numerator,

    COALESCE(SUM(CASE WHEN role='denominator' THEN value_won END), 0) AS denominator,

    SUM(CASE WHEN required THEN 1 ELSE 0 END) AS required_cnt,
    SUM(CASE WHEN required AND value_won IS NOT NULL THEN 1 ELSE 0 END) AS required_hit,

    -- 듀퐁 계산용 원천들(있으면 집계)
    MAX(value_won) FILTER (WHERE item_key='NET_INCOME')     AS ni,
    MAX(value_won) FILTER (WHERE item_key='REVENUE')        AS rev,
    MAX(value_won) FILTER (WHERE item_key='TOTAL_ASSETS')   AS ta,
    MAX(value_won) FILTER (WHERE item_key='EQUITY')         AS eq,
    MAX(value_won) FILTER (WHERE item_key='TAX_RATE')       AS tax_rate

  FROM grid
  GROUP BY corp_code, bsns_year, report_id, ratio_key
)
SELECT
  corp_code,
  bsns_year,
  report_id,
  ratio_key,
  ratio_ko,

  CASE
    WHEN ratio_key = 'dupont_3_factor' THEN
      CASE
        WHEN ni IS NULL OR rev IS NULL OR ta IS NULL OR eq IS NULL THEN NULL
        WHEN rev = 0 OR ta = 0 OR eq = 0 THEN NULL
        ELSE (ni/rev) * (rev/ta) * (ta/eq)
      END

    WHEN ratio_key = 'dupont_4_factor' THEN
      CASE
        WHEN ni IS NULL OR rev IS NULL OR ta IS NULL OR eq IS NULL THEN NULL
        WHEN rev = 0 OR ta = 0 OR eq = 0 THEN NULL
        WHEN tax_rate IS NULL THEN NULL
        ELSE ((ni/rev) * (rev/ta) * (ta/eq)) * (1 - tax_rate)
      END

    ELSE
      CASE
        WHEN required_cnt = required_hit
         AND denominator IS NOT NULL
         AND denominator <> 0
        THEN numerator / denominator
        ELSE NULL
      END
  END AS ratio_value,

  numerator,
  denominator,

  CASE
    WHEN ratio_key IN ('dupont_3_factor','dupont_4_factor') THEN
      (ni IS NOT NULL AND rev IS NOT NULL AND ta IS NOT NULL AND eq IS NOT NULL)
      AND (rev <> 0 AND ta <> 0 AND eq <> 0)
      AND (ratio_key <> 'dupont_4_factor' OR tax_rate IS NOT NULL)
    ELSE
      (required_cnt = required_hit)
  END AS is_complete
FROM agg;
""")
print("✅ v_financial_ratios created")




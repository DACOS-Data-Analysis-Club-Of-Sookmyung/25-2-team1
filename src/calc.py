# src/calc.py
import re
from typing import List


# ============================================================
# 0) label 정규화 함수 (ACCOUNT_MAP와 SQL norm 규칙 일치)
# ============================================================

def norm_label(s: str) -> str:
    s = "" if s is None else str(s)
    s = s.lower()
    s = re.sub(r"[\s\.\,\-\(\)\/\[\]·•:;]+", "", s)
    return s


# ============================================================
# 1) ACCOUNT_MAP
# ============================================================

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
    ("IS_CIS", "TAX_EXP", ["법인세비용", "법인세비용(수익)", "법인세수익(비용)", "법인세수익(비용)"]),
    ("IS_CIS", "NET_INCOME", ["당기순이익", "당기순이익(손실)", "당기순이익(손실)(A)"]),

    ("IS_CIS", "SGA_EXPENSES", ["판매비와관리비", "판관비"]),

    # --- CF ---
    ("CF", "OCF", ["영업활동으로 인한 현금흐름", "영업활동현금흐름", "영업활동으로부터의 현금흐름"]),
    ("CF", "ICF", ["투자활동으로 인한 현금흐름", "투자활동현금흐름"]),
    ("CF", "FCF_FIN", ["재무활동으로 인한 현금흐름", "재무활동현금흐름"]),
    ("CF", "PURCHASE_PPE", ["유형자산의 취득"]),
    ("CF", "PURCHASE_INTANGIBLES", ["무형자산의 취득"]),
    ("CF", "PURCHASE_LT_FIN_ASSETS", ["장기금융상품의 취득"]),
    ("CF", "DISPOSAL_LT_FIN_ASSETS", ["장기금융상품의 처분"]),
]


# ============================================================
# 2) account_map_rules 적재
# ============================================================

def build_account_map_rules(con):
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

    # dedup by PK
    dedup = {}
    for r in rows:
        k = (r["scope"], r["std_key"], r["match_type"], r["pattern"])
        if k not in dedup:
            dedup[k] = r
    rows_dedup = list(dedup.values())

    con.execute("""
    CREATE TABLE IF NOT EXISTS account_map_rules (
      scope VARCHAR,
      std_key VARCHAR,
      match_type VARCHAR,
      pattern VARCHAR,
      pattern_raw VARCHAR,
      priority INTEGER,
      min_indent INTEGER,
      max_indent INTEGER,
      is_active BOOLEAN DEFAULT TRUE,
      note VARCHAR,
      PRIMARY KEY (scope, std_key, match_type, pattern)
    );
    """)

    con.execute("DELETE FROM account_map_rules;")

    con.executemany("""
    INSERT INTO account_map_rules
    (scope, std_key, match_type, pattern, pattern_raw, priority, min_indent, max_indent, is_active, note)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE, NULL)
    """, [
        (r["scope"], r["std_key"], r["match_type"], r["pattern"], r["pattern_raw"],
         r["priority"], r["min_indent"], r["max_indent"])
        for r in rows_dedup
    ])


# ============================================================
# 3) 계산 파이프라인 뷰 생성 (v_fin_long_raw ~ v_financial_ratios)
# ============================================================

def create_calc_views(con):
    # --- v_fin_long_raw ---
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

    # --- v_fin_long_mapped ---
    con.execute("DROP VIEW IF EXISTS v_fin_long_mapped;")
    con.execute(r"""
    CREATE VIEW v_fin_long_mapped AS
    WITH base AS (
      SELECT
        f.*,
        lower(regexp_replace(f.label_clean, '[\s\.\,\-\(\)\/\[\]·•:;]+', '', 'g')) AS label_norm2,
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
       AND r.pattern = b.label_norm2
    )
    SELECT
      corp_code,
      bsns_year,
      report_id,
      table_id,
      row_idx,
      statement_type,
      fiscal_year,
      line_item_id,
      label_clean,
      label_norm2 AS label_norm,
      indent,
      value_won,
      unit_multiplier,
      note_refs_raw,
      std_key,
      matched_pattern_raw,
      priority
    FROM matched
    WHERE rn = 1;
    """)

    # --- v_summary_all_years ---
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

    # --- v_analysis_compare ---
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

    # --- ratio_requirements ---
    con.execute("""
    CREATE TABLE IF NOT EXISTS ratio_requirements (
      ratio_key VARCHAR,
      ratio_ko  VARCHAR,
      item_key  VARCHAR,
      role      VARCHAR,
      required  BOOLEAN,
      note      VARCHAR,
      PRIMARY KEY (ratio_key, item_key, role)
    );
    """)
    con.execute("DELETE FROM ratio_requirements;")

    con.execute(r"""
    INSERT INTO ratio_requirements (ratio_key, ratio_ko, item_key, role, required, note) VALUES
    ('current_ratio', '유동비율', 'CURRENT_ASSETS', 'numerator',  TRUE,  '유동자산/유동부채'),
    ('current_ratio', '유동비율', 'CURRENT_LIABILITIES', 'denominator', TRUE, NULL),

    ('quick_ratio',   '당좌비율', 'CURRENT_ASSETS', 'numerator',  TRUE,  '(유동자산-재고자산)/유동부채'),
    ('quick_ratio',   '당좌비율', 'INVENTORIES',    'subtract',   FALSE, '유동자산에서 차감'),
    ('quick_ratio',   '당좌비율', 'CURRENT_LIABILITIES', 'denominator', TRUE, NULL),

    ('cash_ratio',    '현금비율', 'CASH_EQ', 'numerator', TRUE, '현금및현금성자산/유동부채'),
    ('cash_ratio',    '현금비율', 'CURRENT_LIABILITIES', 'denominator', TRUE, NULL),

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

    ('asset_turnover', '자산 회전율', 'REVENUE', 'numerator', TRUE, '매출/총자산'),
    ('asset_turnover', '자산 회전율', 'TOTAL_ASSETS', 'denominator', TRUE, NULL),

    ('inventory_turnover', '재고자산 회전율', 'COGS', 'numerator', TRUE, '매출원가/재고자산'),
    ('inventory_turnover', '재고자산 회전율', 'INVENTORIES', 'denominator', TRUE, NULL),

    ('ar_turnover', '매출채권 회전율', 'REVENUE', 'numerator', TRUE, '매출/매출채권'),
    ('ar_turnover', '매출채권 회전율', 'AR', 'denominator', TRUE, NULL),

    ('roe', 'ROE', 'NET_INCOME', 'numerator', TRUE, '순이익/자기자본'),
    ('roe', 'ROE', 'EQUITY', 'denominator', TRUE, NULL),

    ('roa', 'ROA', 'NET_INCOME', 'numerator', TRUE, '순이익/총자산'),
    ('roa', 'ROA', 'TOTAL_ASSETS', 'denominator', TRUE, NULL),

    ('roc', 'ROC', 'NOPAT', 'numerator', TRUE, '세후영업이익/(장기부채+자기자본)'),
    ('roc', 'ROC', 'INVESTED_CAPITAL', 'denominator', TRUE, NULL),

    ('per',  'PER',  'STOCK_PRICE',  'numerator', TRUE,  '주가/주당순이익'),
    ('per',  'PER',  'EPS',          'denominator', TRUE,  NULL),

    ('pbr',  'PBR',  'STOCK_PRICE',  'numerator', TRUE,  '주가/주당자기자본'),
    ('pbr',  'PBR',  'BPS',          'denominator', TRUE,  NULL),

    ('psr',  'PSR',  'STOCK_PRICE',  'numerator', TRUE,  '주가/주당매출'),
    ('psr',  'PSR',  'SPS',          'denominator', TRUE,  NULL),

    ('pcfr', 'PCFR', 'STOCK_PRICE',  'numerator', TRUE,  '주가/주당(세후 순이익+감가상각비)'),
    ('pcfr', 'PCFR', 'CFPS',         'denominator', TRUE,  NULL),

    ('net_margin', '순이익률', 'NET_INCOME', 'numerator',   TRUE, '당기순이익/매출액'),
    ('net_margin', '순이익률', 'REVENUE',    'denominator', TRUE, NULL),

    ('fin_leverage', '재무레버리지', 'TOTAL_ASSETS', 'numerator',   TRUE, '자산총계/자본총계'),
    ('fin_leverage', '재무레버리지', 'EQUITY',       'denominator', TRUE, NULL)
    ;
    """)

    # --- v_value_resolved ---
    con.execute("DROP VIEW IF EXISTS v_value_resolved;")
    con.execute(r"""
    CREATE VIEW v_value_resolved AS
    WITH req AS (
      SELECT DISTINCT item_key AS std_key
      FROM ratio_requirements
      WHERE item_key NOT IN (
        'EPS','BPS','SPS','CFPS',
        'NOPAT','INVESTED_CAPITAL','TAX_RATE'
      )
      UNION ALL
      SELECT * FROM (VALUES
        ('SHARES_OUTSTANDING'),
        ('STOCK_PRICE'),
        ('PRE_TAX_INCOME'),
        ('TAX_EXP'),
        ('DEPRECIATION')
      ) t(std_key)
    ),
    std_scope AS (
      SELECT std_key, MIN(scope) AS scope
      FROM account_map_rules
      WHERE is_active = TRUE
      GROUP BY std_key
    ),
    base_reports AS (
      SELECT DISTINCT corp_code, bsns_year, report_id
      FROM v_fin_long_mapped
    ),
    market_core AS (
      SELECT
        corp_code,
        CAST(year AS INT) AS bsns_year,
        arg_max(stock_price, asof_date)        AS stock_price,
        arg_max(shares_outstanding, asof_date) AS shares_outstanding
      FROM market_data
      GROUP BY 1,2
    ),
    candidates AS (
      SELECT
        m.corp_code,
        m.bsns_year,
        m.report_id,
        m.std_key,
        m.value_won,
        m.label_clean,
        m.note_refs_raw,
        m.line_item_id
      FROM v_fin_long_mapped m
      JOIN std_scope s
        ON s.std_key = m.std_key
       AND s.scope   = m.statement_type
      JOIN account_map_rules r
        ON r.scope   = m.statement_type
       AND r.std_key = m.std_key
       AND r.match_type = 'EXACT'
       AND r.pattern = m.label_norm
      WHERE m.std_key IS NOT NULL
        AND m.fiscal_year = m.bsns_year
    ),
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
        list(DISTINCT label_clean) AS labels,
        list(DISTINCT note_refs_raw) AS note_refs,
        COUNT(*) AS cand_rows,
        COUNT(DISTINCT value_won) AS cand_distinct_values,
        list(DISTINCT line_item_id) AS line_item_ids
      FROM candidates
      GROUP BY 1,2,3,4
    )
    SELECT
      br.corp_code,
      br.bsns_year,
      br.report_id,
      r.std_key,
      COALESCE(
        bv.value_won,
        CASE WHEN r.std_key='STOCK_PRICE'        THEN mk.stock_price END,
        CASE WHEN r.std_key='SHARES_OUTSTANDING' THEN mk.shares_outstanding END
      ) AS value_won,
      bv.labels,
      bv.note_refs,
      NULL AS note_text,
      bv.cand_rows,
      bv.cand_distinct_values
    FROM base_reports br
    CROSS JOIN req r
    LEFT JOIN base_vals bv
      ON bv.corp_code = br.corp_code
     AND bv.bsns_year = br.bsns_year
     AND bv.report_id = br.report_id
     AND bv.std_key   = r.std_key
    LEFT JOIN market_core mk
      ON mk.corp_code = br.corp_code
     AND mk.bsns_year = br.bsns_year
    ;
    """)

    # --- v_value_augmented (dedup) ---
    con.execute("DROP VIEW IF EXISTS v_value_augmented;")
    con.execute(r"""
    CREATE VIEW v_value_augmented AS
    WITH base AS (
      SELECT
        corp_code,
        bsns_year,
        report_id,

        MAX(value_won) FILTER (WHERE std_key='OP_PROFIT')               AS op_profit,
        MAX(value_won) FILTER (WHERE std_key='TAX_EXP')                 AS tax_exp,
        MAX(value_won) FILTER (WHERE std_key='PRE_TAX_INCOME')          AS pre_tax_income,

        MAX(value_won) FILTER (WHERE std_key='LONG_TERM_DEBT')          AS long_term_debt,
        MAX(value_won) FILTER (WHERE std_key='NON_CURRENT_LIABILITIES') AS non_current_liabilities,
        MAX(value_won) FILTER (WHERE std_key='TOTAL_LIABILITIES')       AS total_liabilities,
        MAX(value_won) FILTER (WHERE std_key='CURRENT_LIABILITIES')     AS current_liabilities,

        MAX(value_won) FILTER (WHERE std_key='EQUITY')                  AS equity,
        MAX(value_won) FILTER (WHERE std_key='TOTAL_ASSETS')            AS total_assets,

        MAX(value_won) FILTER (WHERE std_key='NET_INCOME')              AS net_income,
        MAX(value_won) FILTER (WHERE std_key='REVENUE')                 AS revenue,
        MAX(value_won) FILTER (WHERE std_key='DEPRECIATION')            AS depreciation,

        MAX(value_won) FILTER (WHERE std_key='STOCK_PRICE')             AS stock_price,
        MAX(value_won) FILTER (WHERE std_key='SHARES_OUTSTANDING')      AS shares_outstanding

      FROM v_value_resolved
      GROUP BY corp_code, bsns_year, report_id
    ),
    derived AS (
      SELECT
        b.*,

        CASE
          WHEN tax_exp IS NULL OR pre_tax_income IS NULL OR pre_tax_income = 0 THEN NULL
          ELSE
            CASE
              WHEN (tax_exp / pre_tax_income) < 0 THEN 0
              WHEN (tax_exp / pre_tax_income) > 1 THEN 1
              ELSE (tax_exp / pre_tax_income)
            END
        END AS tax_rate,

        CASE
          WHEN op_profit IS NULL THEN NULL
          WHEN tax_exp IS NULL OR pre_tax_income IS NULL OR pre_tax_income = 0 THEN NULL
          ELSE op_profit * (1 - (
            CASE
              WHEN (tax_exp / pre_tax_income) < 0 THEN 0
              WHEN (tax_exp / pre_tax_income) > 1 THEN 1
              ELSE (tax_exp / pre_tax_income)
            END
          ))
        END AS nopat,

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

        CASE WHEN net_income IS NOT NULL AND shares_outstanding IS NOT NULL AND shares_outstanding <> 0
             THEN net_income / shares_outstanding ELSE NULL END AS eps,

        CASE WHEN equity IS NOT NULL AND shares_outstanding IS NOT NULL AND shares_outstanding <> 0
             THEN equity / shares_outstanding ELSE NULL END AS bps,

        CASE WHEN revenue IS NOT NULL AND shares_outstanding IS NOT NULL AND shares_outstanding <> 0
             THEN revenue / shares_outstanding ELSE NULL END AS sps,

        CASE WHEN net_income IS NOT NULL AND shares_outstanding IS NOT NULL AND shares_outstanding <> 0
             THEN (net_income + COALESCE(depreciation,0)) / shares_outstanding ELSE NULL END AS cfps
      FROM base b
    ),
    raw_rows AS (
      SELECT
        corp_code, bsns_year, report_id,
        std_key,
        value_won,
        labels,
        note_refs,
        note_text,
        2 AS prio
      FROM v_value_resolved
    ),
    derived_rows AS (
      SELECT corp_code, bsns_year, report_id, 'TAX_RATE' AS std_key, tax_rate AS value_won, NULL, NULL, NULL, 1 AS prio FROM derived
      UNION ALL SELECT corp_code, bsns_year, report_id, 'NOPAT' AS std_key, nopat AS value_won, NULL, NULL, NULL, 1 FROM derived
      UNION ALL SELECT corp_code, bsns_year, report_id, 'INVESTED_CAPITAL' AS std_key, invested_capital AS value_won, NULL, NULL, NULL, 1 FROM derived
      UNION ALL SELECT corp_code, bsns_year, report_id, 'EPS' AS std_key, eps AS value_won, NULL, NULL, NULL, 1 FROM derived
      UNION ALL SELECT corp_code, bsns_year, report_id, 'BPS' AS std_key, bps AS value_won, NULL, NULL, NULL, 1 FROM derived
      UNION ALL SELECT corp_code, bsns_year, report_id, 'SPS' AS std_key, sps AS value_won, NULL, NULL, NULL, 1 FROM derived
      UNION ALL SELECT corp_code, bsns_year, report_id, 'CFPS' AS std_key, cfps AS value_won, NULL, NULL, NULL, 1 FROM derived
    ),
    unioned AS (
      SELECT * FROM raw_rows
      UNION ALL
      SELECT * FROM derived_rows
    ),
    dedup AS (
      SELECT *
      FROM unioned
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY corp_code, bsns_year, report_id, std_key
        ORDER BY prio ASC, (value_won IS NULL) ASC
      ) = 1
    )
    SELECT
      corp_code, bsns_year, report_id,
      std_key,
      value_won,
      labels,
      note_refs,
      note_text
    FROM dedup;
    """)

    # --- v_financial_ratios ---
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
        SUM(CASE WHEN required AND value_won IS NOT NULL THEN 1 ELSE 0 END) AS required_hit
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
        WHEN required_cnt = required_hit
         AND denominator IS NOT NULL
         AND denominator <> 0
        THEN numerator / denominator
        ELSE NULL
      END AS ratio_value,
      numerator,
      denominator,
      (required_cnt = required_hit) AS is_complete
    FROM agg;
    """)


# ============================================================
# 4) metric_catalog
# ============================================================

def create_metric_catalog(con):
    con.execute("""
    DROP TABLE IF EXISTS metric_catalog;
    CREATE TABLE metric_catalog (
      metric_key VARCHAR PRIMARY KEY,
      metric_name_ko VARCHAR,
      metric_type VARCHAR,        -- raw / ratio / derived / market
      unit VARCHAR,               -- KRW / RATIO / TIMES / KRW_PER_SHARE ...
      polarity BOOLEAN            -- TRUE: higher_better, FALSE: lower_better, NULL: depends
    );
    """)

    con.execute("""
    INSERT INTO metric_catalog VALUES
    ('TOTAL_ASSETS', '자산총계', 'raw', 'KRW', TRUE),
    ('CURRENT_ASSETS', '유동자산', 'raw', 'KRW', TRUE),
    ('CASH_EQ', '현금및현금성자산', 'raw', 'KRW', TRUE),
    ('AR', '매출채권', 'raw', 'KRW', NULL),
    ('INVENTORIES', '재고자산', 'raw', 'KRW', NULL),
    ('NON_CURRENT_ASSETS', '비유동자산', 'raw', 'KRW', TRUE),
    ('LONG_TERM_DEBT', '장기차입금', 'raw', 'KRW', FALSE),
    ('DEFERRED_TAX_LIAB', '이연법인세부채', 'raw', 'KRW', FALSE),

    ('EQUITY', '자본총계', 'raw', 'KRW', TRUE),
    ('PARENT_EQUITY', '지배기업 소유주지분', 'raw', 'KRW', TRUE),
    ('RETAINED_EARNINGS', '이익잉여금', 'raw', 'KRW', TRUE),
    ('NON_CONTROLLING_INTEREST', '비지배지분', 'raw', 'KRW', TRUE),
    ('CAPITAL_STOCK', '자본금', 'raw', 'KRW', TRUE),

    ('REVENUE', '매출액', 'raw', 'KRW', TRUE),
    ('COGS', '매출원가', 'raw', 'KRW', FALSE),
    ('GROSS_PROFIT', '매출총이익', 'raw', 'KRW', TRUE),
    ('OP_PROFIT', '영업이익', 'raw', 'KRW', TRUE),
    ('PRE_TAX_INCOME', '법인세비용차감전순이익', 'raw', 'KRW', TRUE),
    ('TAX_EXP', '법인세비용', 'raw', 'KRW', FALSE),
    ('NET_INCOME', '당기순이익', 'raw', 'KRW', TRUE),
    ('SGA_EXPENSES', '판매비와관리비', 'raw', 'KRW', FALSE),

    ('OCF', '영업활동현금흐름', 'raw', 'KRW', TRUE),
    ('ICF', '투자활동현금흐름', 'raw', 'KRW', NULL),
    ('FCF_FIN', '재무활동현금흐름', 'raw', 'KRW', NULL),
    ('PURCHASE_PPE', '유형자산의 취득', 'raw', 'KRW', NULL),
    ('PURCHASE_INTANGIBLES', '무형자산의 취득', 'raw', 'KRW', NULL),
    ('PURCHASE_LT_FIN_ASSETS', '장기금융상품의 취득', 'raw', 'KRW', NULL),
    ('DISPOSAL_LT_FIN_ASSETS', '장기금융상품의 처분', 'raw', 'KRW', TRUE),

    ('roe', 'ROE', 'ratio', 'RATIO', TRUE),
    ('roa', 'ROA', 'ratio', 'RATIO', TRUE),
    ('roc', 'ROC', 'ratio', 'RATIO', TRUE),
    ('long_term_debt_ratio', '장기부채비율', 'ratio', 'RATIO', FALSE),
    ('net_margin', '순이익률', 'ratio', 'RATIO', TRUE),
    ('per', 'PER', 'ratio', 'TIMES', NULL),
    ('pbr', 'PBR', 'ratio', 'TIMES', NULL),
    ('psr', 'PSR', 'ratio', 'TIMES', NULL),
    ('pcfr', 'PCFR', 'ratio', 'TIMES', NULL),

    ('EPS', 'EPS', 'derived', 'KRW_PER_SHARE', TRUE),
    ('BPS', 'BPS', 'derived', 'KRW_PER_SHARE', TRUE),
    ('SPS', 'SPS', 'derived', 'KRW_PER_SHARE', TRUE),
    ('CFPS', 'CFPS', 'derived', 'KRW_PER_SHARE', TRUE),
    ('STOCK_PRICE', '기말 주가', 'market', 'KRW_PER_SHARE', TRUE)
    ;
    """)


# ============================================================
# 5) fact_metrics
# ============================================================

def create_fact_metrics_table(con):
    # ✅ DROP 금지 (target → benchmark 순서로 적재 시 테이블이 날아가면 bench 비교가 누락됨)
    con.execute("""
    CREATE TABLE IF NOT EXISTS fact_metrics (
      corp_code VARCHAR,
      bsns_year INTEGER,
      metric_key VARCHAR,
      metric_name_ko VARCHAR,
      metric_type VARCHAR,
      value DOUBLE,
      value_prev DOUBLE,
      yoy_abs DOUBLE,
      yoy_pct DOUBLE,
      unit VARCHAR,
      benchmark_corp_code VARCHAR,
      benchmark_value DOUBLE,
      benchmark_improved BOOLEAN,
      PRIMARY KEY (corp_code, bsns_year, metric_key)
    );
    """)


def load_fact_metrics(con, corp_code: str, bsns_year: int, metrics_spec: List[str]) -> None:
    """
    요청 범위(corp_code / bsns_year / metrics_spec)에 해당하는 지표만 fact_metrics에 적재
    - SSOT: request_metrics TEMP 테이블
    - ratio/derived/market은 report_id 중복 가능 → QUALIFY로 dedup 후 적재
    """
    if not metrics_spec:
        raise ValueError("metrics_spec is empty")

    create_fact_metrics_table(con)

    # 요청 범위만 clear
    con.execute(
        "DELETE FROM fact_metrics WHERE corp_code = ? AND bsns_year = ?",
        [corp_code, bsns_year],
    )

    # ----------------------------
    # RAW
    # ----------------------------
    con.execute(
        """
        INSERT INTO fact_metrics
        SELECT
          a.corp_code,
          a.bsns_year,
          a.std_key                 AS metric_key,
          mc.metric_name_ko,
          'raw'                     AS metric_type,
          a.val_curr                AS value,
          a.val_prev                AS value_prev,
          a.diff_amt                AS yoy_abs,
          (a.diff_rate / 100.0)     AS yoy_pct,
          mc.unit,
          NULL, NULL, NULL
        FROM v_analysis_compare a
        JOIN metric_catalog mc
          ON mc.metric_key = a.std_key
        WHERE a.corp_code = ?
          AND a.bsns_year = ?
          AND mc.metric_type = 'raw'
          AND EXISTS (SELECT 1 FROM request_metrics rm WHERE rm.metric_key = a.std_key);
        """,
        [corp_code, bsns_year],
    )

    # ----------------------------
    # RATIO (dedup)
    # ----------------------------
    con.execute(
        """
        WITH cur_dedup AS (
          SELECT
            cur.corp_code,
            cur.bsns_year,
            cur.ratio_key,
            cur.ratio_value,
            cur.report_id
          FROM v_financial_ratios cur
          WHERE cur.corp_code = ?
            AND cur.bsns_year = ?
            AND EXISTS (SELECT 1 FROM request_metrics rm WHERE rm.metric_key = cur.ratio_key)
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY cur.corp_code, cur.bsns_year, cur.ratio_key
            ORDER BY (cur.ratio_value IS NOT NULL) DESC, abs(cur.ratio_value) DESC, cur.report_id DESC
          ) = 1
        ),
        prev_dedup AS (
          SELECT
            prev.corp_code,
            prev.bsns_year,
            prev.ratio_key,
            prev.ratio_value,
            prev.report_id
          FROM v_financial_ratios prev
          WHERE prev.corp_code = ?
            AND prev.bsns_year = ? - 1
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY prev.corp_code, prev.bsns_year, prev.ratio_key
            ORDER BY (prev.ratio_value IS NOT NULL) DESC, abs(prev.ratio_value) DESC, prev.report_id DESC
          ) = 1
        )
        INSERT INTO fact_metrics
        SELECT
          c.corp_code,
          c.bsns_year,
          c.ratio_key                  AS metric_key,
          mc.metric_name_ko,
          'ratio'                      AS metric_type,
          c.ratio_value                AS value,
          p.ratio_value                AS value_prev,
          (c.ratio_value - p.ratio_value) AS yoy_abs,
          CASE
            WHEN p.ratio_value IS NOT NULL AND p.ratio_value != 0
            THEN (c.ratio_value - p.ratio_value) / abs(p.ratio_value)
            ELSE NULL
          END AS yoy_pct,
          mc.unit,
          NULL, NULL, NULL
        FROM cur_dedup c
        LEFT JOIN prev_dedup p
          ON c.corp_code = p.corp_code
         AND c.ratio_key = p.ratio_key
        JOIN metric_catalog mc
          ON mc.metric_key = c.ratio_key
        WHERE mc.metric_type = 'ratio';
        """,
        [corp_code, bsns_year, corp_code, bsns_year],
    )

    # ----------------------------
    # DERIVED / MARKET (dedup)
    # ----------------------------
    con.execute(
        """
        WITH cur_dedup AS (
          SELECT
            cur.corp_code,
            cur.bsns_year,
            cur.std_key,
            cur.value_won,
            cur.report_id
          FROM v_value_augmented cur
          WHERE cur.corp_code = ?
            AND cur.bsns_year = ?
            AND EXISTS (SELECT 1 FROM request_metrics rm WHERE rm.metric_key = cur.std_key)
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY cur.corp_code, cur.bsns_year, cur.std_key
            ORDER BY (cur.value_won IS NOT NULL) DESC, abs(cur.value_won) DESC, cur.report_id DESC
          ) = 1
        ),
        prev_dedup AS (
          SELECT
            prev.corp_code,
            prev.bsns_year,
            prev.std_key,
            prev.value_won,
            prev.report_id
          FROM v_value_augmented prev
          WHERE prev.corp_code = ?
            AND prev.bsns_year = ? - 1
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY prev.corp_code, prev.bsns_year, prev.std_key
            ORDER BY (prev.value_won IS NOT NULL) DESC, abs(prev.value_won) DESC, prev.report_id DESC
          ) = 1
        )
        INSERT INTO fact_metrics
        SELECT
          c.corp_code,
          c.bsns_year,
          c.std_key                    AS metric_key,
          mc.metric_name_ko,
          mc.metric_type,
          c.value_won                  AS value,
          p.value_won                  AS value_prev,
          (c.value_won - p.value_won)  AS yoy_abs,
          CASE
            WHEN p.value_won IS NOT NULL AND p.value_won != 0
            THEN (c.value_won - p.value_won) / abs(p.value_won)
            ELSE NULL
          END AS yoy_pct,
          mc.unit,
          NULL, NULL, NULL
        FROM cur_dedup c
        LEFT JOIN prev_dedup p
          ON c.corp_code = p.corp_code
         AND c.std_key   = p.std_key
        JOIN metric_catalog mc
          ON mc.metric_key = c.std_key
        WHERE mc.metric_type IN ('derived','market');
        """,
        [corp_code, bsns_year, corp_code, bsns_year],
    )


def update_benchmark_values(con, corp_code: str, bsns_year: int, metrics_spec: List[str]) -> str:
    """
    benchmark_map을 기반으로 fact_metrics에 benchmark_corp_code / benchmark_value 채우기
    전제: 벤치 기업도 동일 metric_key로 fact_metrics가 이미 적재되어 있어야 함
    """
    if not metrics_spec:
        raise ValueError("metrics_spec is empty")

    row = con.execute(
        """
        SELECT bench_corp_code
        FROM benchmark_map
        WHERE corp_code = ?
          AND year = ?
        """,
        [corp_code, bsns_year],
    ).fetchone()

    if not row or not row[0]:
        raise RuntimeError(f"benchmark_map에 벤치 기업이 없습니다: corp_code={corp_code}, year={bsns_year}")

    bench_corp_code = row[0]

    con.execute(
        """
        UPDATE fact_metrics f
        SET
          benchmark_corp_code = ?,
          benchmark_value = b.value
        FROM fact_metrics b
        WHERE f.corp_code = ?
          AND f.bsns_year = ?
          AND b.corp_code = ?
          AND b.bsns_year = ?
          AND f.metric_key = b.metric_key
          AND EXISTS (SELECT 1 FROM request_metrics rm WHERE rm.metric_key = f.metric_key);
        """,
        [bench_corp_code, corp_code, bsns_year, bench_corp_code, bsns_year],
    )

    print(f"✅ benchmark_value updated: corp={corp_code}, year={bsns_year}, bench={bench_corp_code}")
    return bench_corp_code


def update_benchmark_improved(con, corp_code: str, bsns_year: int, metrics_spec: List[str]) -> None:
    """
    metric_catalog.polarity 규칙으로 benchmark_improved 계산
    """
    if not metrics_spec:
        raise ValueError("metrics_spec is empty")

    con.execute(
        """
        UPDATE fact_metrics f
        SET benchmark_improved =
          CASE
            WHEN f.benchmark_value IS NULL THEN NULL
            WHEN mc.polarity IS NULL THEN NULL
            WHEN mc.polarity = TRUE  THEN (f.value >= f.benchmark_value)
            WHEN mc.polarity = FALSE THEN (f.value <= f.benchmark_value)
            ELSE NULL
          END
        FROM metric_catalog mc
        WHERE f.metric_key = mc.metric_key
          AND f.corp_code = ?
          AND f.bsns_year = ?
          AND EXISTS (SELECT 1 FROM request_metrics rm WHERE rm.metric_key = f.metric_key);
        """,
        [corp_code, bsns_year],
    )

    print("✅ benchmark_improved updated (request_metrics scope)")

-- Data marts for Metabase. Built by etl/marts.py from the warehouse tables.
--
-- Each block starts with "-- @mart <name>" and is ONE statement. Blocks run in
-- order, inside one transaction, into a freshly rebuilt marts schema, so a
-- re-run always gives the same tables and a failure leaves the old ones intact.
--
-- Placeholders: warehouse warehouse schema, marts marts schema, {shock_values} the
-- simulated income drops. come from etl/config.py.
--
-- Definitions used throughout
--   month            calendar month of the transaction date
--   partial month    the first or last observed month of a household when its
--                    observation does not cover the whole month
--   complete month   every other observed month. Month-level measures (negative
--                    months, streaks, volatility, seasonality) use complete months
--                    only. Totals and ratios use all observed months.
--   income           operating income (gross); income costs (farm or business
--                    costs booked in the income class) are kept separately
--   outflow          expenditure (opex) + income costs
--   net flow         income - income costs - expenditure  (= operating_net_kes)
--   borrowing        new borrowing on a financial liability; formal = formal loan,
--                    employer = employer loan or advance, informal = store credit,
--                    group and person-to-person loans


-- @mart mart_household_month
-- One row per household per observed month. The base of most other marts.
CREATE TABLE marts.mart_household_month AS
WITH bounds AS (
    SELECT household_key, hh_id, first_trx_date, last_trx_date,
           date_trunc('month', first_trx_date)::date AS first_month,
           date_trunc('month', last_trx_date)::date  AS last_month
    FROM warehouse.dim_household
), months AS (
    SELECT year_month, MIN(full_date) AS month_start, MIN(season) AS season
    FROM warehouse.dim_date GROUP BY year_month
), spine AS (
    SELECT b.household_key, b.hh_id, m.year_month, m.month_start, m.season,
           (   (m.month_start = b.first_month AND b.first_trx_date > b.first_month)
            OR (m.month_start = b.last_month
                AND b.last_trx_date < (b.last_month + INTERVAL '1 month' - INTERVAL '1 day')::date)
           ) AS is_partial_month
    FROM bounds b
    JOIN months m ON m.month_start BETWEEN b.first_month AND b.last_month
), agg AS (
    SELECT f.household_key, d.year_month,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'operating_income')   AS income_kes,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'income_cost')        AS income_cost_kes,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'operating_expense')  AS expenditure_kes,
        SUM(f.operating_net_kes)                                               AS operating_net_kes,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'savings_deposit')    AS savings_deposit_kes,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'savings_withdrawal') AS savings_withdrawal_kes,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'borrowing')          AS borrowing_kes,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'borrowing'
                                   AND t.instrument_group = 'formal_loan')     AS borrowing_formal_kes,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'borrowing'
                                   AND t.instrument_group = 'employer_credit') AS borrowing_employer_kes,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'borrowing'
                                   AND t.instrument_group IN ('informal_credit', 'informal_group', 'informal_p2p'))
                                                                               AS borrowing_informal_kes,
        SUM(f.value_kes) FILTER (WHERE f.flow_category = 'repayment')          AS repayment_kes,
        COUNT(*)                                          AS n_transactions,
        COUNT(*) FILTER (WHERE f.mode_desc IS NOT NULL)   AS n_mode_transactions,
        COUNT(*) FILTER (WHERE f.is_cash)                 AS n_cash_transactions,
        COUNT(*) FILTER (WHERE f.is_digital)              AS n_digital_transactions,
        COUNT(DISTINCT COALESCE(t.income_source_group, 'other'))
              FILTER (WHERE f.flow_category = 'operating_income' AND f.value_kes > 0) AS income_source_count
    FROM warehouse.fact_transaction f
    JOIN warehouse.dim_date d      ON d.date_key = f.date_key
    JOIN warehouse.dim_trx_type t  ON t.trx_type_key = f.trx_type_key
    GROUP BY f.household_key, d.year_month
)
SELECT s.household_key, s.hh_id, s.year_month, s.month_start, s.season,
       s.is_partial_month, NOT s.is_partial_month AS is_complete_month,
       COALESCE(a.income_kes, 0)            AS income_kes,
       COALESCE(a.income_cost_kes, 0)       AS income_cost_kes,
       COALESCE(a.expenditure_kes, 0)       AS expenditure_kes,
       COALESCE(a.operating_net_kes, 0)     AS operating_net_kes,
       COALESCE(a.operating_net_kes, 0) < 0 AS is_negative_month,
       COALESCE(a.savings_deposit_kes, 0)    AS savings_deposit_kes,
       COALESCE(a.savings_withdrawal_kes, 0) AS savings_withdrawal_kes,
       COALESCE(a.borrowing_kes, 0)          AS borrowing_kes,
       COALESCE(a.borrowing_formal_kes, 0)   AS borrowing_formal_kes,
       COALESCE(a.borrowing_employer_kes, 0) AS borrowing_employer_kes,
       COALESCE(a.borrowing_informal_kes, 0) AS borrowing_informal_kes,
       COALESCE(a.repayment_kes, 0)          AS repayment_kes,
       COALESCE(a.n_transactions, 0)         AS n_transactions,
       COALESCE(a.n_mode_transactions, 0)    AS n_mode_transactions,
       COALESCE(a.n_cash_transactions, 0)    AS n_cash_transactions,
       COALESCE(a.n_digital_transactions, 0) AS n_digital_transactions,
       COALESCE(a.income_source_count, 0)    AS income_source_count
FROM spine s
LEFT JOIN agg a ON a.household_key = s.household_key AND a.year_month = s.year_month;

-- @mart mart_household_resilience
-- One row per household: how often and how long net cash flow is negative, and
-- how volatile income and spending are. Answers "which households experience
-- persistent negative net cash flow" with BOTH rules, because the choice of rule
-- changes the answer: rule A = negative in more tha of
-- complete months,  or more negative months in a row.
CREATE TABLE marts.mart_household_resilience AS
WITH params AS (
    SELECT 
        6::int         AS min_complete_months,
        0.5::numeric   AS target_negative_share,
        3::int         AS target_streak_len
), cm AS (
    SELECT * FROM marts.mart_household_month WHERE is_complete_month
), ordered AS (
    SELECT household_key, is_negative_month,
           ROW_NUMBER() OVER (PARTITION BY household_key ORDER BY month_start) AS rn,
           ROW_NUMBER() OVER (PARTITION BY household_key, is_negative_month ORDER BY month_start) AS rn_in_state
    FROM cm
), streaks AS (
    SELECT household_key, COUNT(*) AS streak_len
    FROM ordered WHERE is_negative_month
    GROUP BY household_key, (rn - rn_in_state)
), longest AS (
    SELECT household_key, MAX(streak_len) AS max_negative_streak FROM streaks GROUP BY household_key
), stats AS (
    SELECT household_key,
           COUNT(*)                                     AS complete_months,
           COUNT(*) FILTER (WHERE is_negative_month)      AS negative_months,
           AVG(income_kes)                              AS avg_monthly_income_kes,
           AVG(expenditure_kes)                         AS avg_monthly_expenditure_kes,
           AVG(income_cost_kes)                         AS avg_monthly_income_cost_kes,
           AVG(operating_net_kes)                       AS avg_monthly_net_kes,
           STDDEV_SAMP(income_kes)      / NULLIF(AVG(income_kes), 0)      AS income_volatility,
           STDDEV_SAMP(expenditure_kes) / NULLIF(AVG(expenditure_kes), 0) AS expenditure_volatility,
           COUNT(*) FILTER (WHERE borrowing_kes > 0)      AS months_with_borrowing,
           COUNT(*) FILTER (WHERE savings_deposit_kes > 0) AS months_with_savings
    FROM cm GROUP BY household_key
), all_months AS (
    SELECT household_key, COUNT(*) AS months_observed
    FROM marts.mart_household_month GROUP BY household_key
), flags AS (
    SELECT h.household_key, h.hh_id,
           COALESCE(am.months_observed, 0)                AS months_observed,
           COALESCE(s.complete_months, 0)                 AS complete_months,
           COALESCE(s.negative_months, 0)                 AS negative_months,
           s.negative_months::numeric / NULLIF(s.complete_months, 0) AS negative_month_share,
           COALESCE(l.max_negative_streak, 0)             AS max_negative_streak,
           COALESCE(s.complete_months, 0) >= p.min_complete_months AS enough_months,
           s.avg_monthly_income_kes, s.avg_monthly_expenditure_kes,
           s.avg_monthly_income_cost_kes,
           s.avg_monthly_expenditure_kes + s.avg_monthly_income_cost_kes AS avg_monthly_outflow_kes,
           s.avg_monthly_net_kes, s.income_volatility, s.expenditure_volatility,
           COALESCE(s.months_with_borrowing, 0)           AS months_with_borrowing,
           COALESCE(s.months_with_savings, 0)             AS months_with_savings,
           s.months_with_borrowing::numeric / NULLIF(s.complete_months, 0) AS share_months_with_borrowing,
           p.target_negative_share, p.target_streak_len
    FROM warehouse.dim_household h
    LEFT JOIN all_months am ON am.household_key = h.household_key
    LEFT JOIN stats s        ON s.household_key  = h.household_key
    LEFT JOIN longest l      ON l.household_key  = h.household_key
    CROSS JOIN params p
)
SELECT f.household_key, f.hh_id, f.months_observed, f.complete_months, f.negative_months,
       f.negative_month_share, f.max_negative_streak, f.enough_months,
       f.avg_monthly_income_kes, f.avg_monthly_expenditure_kes, f.avg_monthly_income_cost_kes,
       f.avg_monthly_outflow_kes, f.avg_monthly_net_kes, f.income_volatility, f.expenditure_volatility,
       f.months_with_borrowing, f.months_with_savings, f.share_months_with_borrowing,
       f.negative_month_share > f.target_negative_share                       AS persistent_by_share,
       f.max_negative_streak >= f.target_streak_len                           AS persistent_by_streak,
       (f.negative_month_share > f.target_negative_share) OR (f.max_negative_streak >= f.target_streak_len)
                                                                             AS persistent_either,
       (f.negative_month_share > f.target_negative_share) AND (f.max_negative_streak >= f.target_streak_len)
                                                                             AS persistent_both,
       CASE WHEN NOT f.enough_months THEN 'insufficient data'
            WHEN (f.negative_month_share > f.target_negative_share) AND (f.max_negative_streak >= f.target_streak_len)
                 THEN 'persistent: both rules'
            WHEN (f.negative_month_share > f.target_negative_share) OR (f.max_negative_streak >= f.target_streak_len)
                 THEN 'persistent: one rule'
            ELSE 'not persistent' END                                         AS vulnerability_tier
FROM flags f;

-- @mart mart_household_inclusion
-- One row per household: use of formal vs informal finance, payment modes.
-- Formal/informal is a property of the account, so it exists only on finance
-- (findev) transactions. Rows on accounts with no formal/informal label are
-- counted separately and do not count as either.
CREATE TABLE marts.mart_household_inclusion AS
WITH fin AS (
    SELECT f.household_key, f.account_key, a.formality, f.value_kes, d.year_month
    FROM warehouse.fact_transaction f
    JOIN warehouse.dim_account a   ON a.account_key = f.account_key
    JOIN warehouse.dim_trx_type t  ON t.trx_type_key = f.trx_type_key
    JOIN warehouse.dim_date d      ON d.date_key = f.date_key
    WHERE t.class_code = 'findev'
), agg AS (
    SELECT household_key,
           COUNT(*) FILTER (WHERE formality = 'Formal')   AS formal_txns,
           COUNT(*) FILTER (WHERE formality = 'Informal') AS informal_txns,
           COUNT(*) FILTER (WHERE formality IS NULL)      AS unlabelled_account_txns,
           SUM(value_kes) FILTER (WHERE formality = 'Formal')   AS formal_value_kes,
           SUM(value_kes) FILTER (WHERE formality = 'Informal') AS informal_value_kes,
           COUNT(DISTINCT account_key) FILTER (WHERE formality = 'Formal')   AS formal_accounts,
           COUNT(DISTINCT account_key) FILTER (WHERE formality = 'Informal') AS informal_accounts,
           MIN(year_month) FILTER (WHERE formality = 'Formal') AS first_formal_month
    FROM fin GROUP BY household_key
), modes AS (
    SELECT household_key,
           SUM(n_mode_transactions) AS n_mode, SUM(n_cash_transactions) AS n_cash,
           SUM(n_digital_transactions) AS n_digital
    FROM marts.mart_household_month GROUP BY household_key
)
SELECT h.household_key, h.hh_id,
       COALESCE(a.formal_txns, 0)   AS formal_txns,
       COALESCE(a.informal_txns, 0) AS informal_txns,
       COALESCE(a.unlabelled_account_txns, 0) AS unlabelled_account_txns,
       COALESCE(a.formal_value_kes, 0)   AS formal_value_kes,
       COALESCE(a.informal_value_kes, 0) AS informal_value_kes,
       COALESCE(a.formal_accounts, 0)    AS formal_accounts,
       COALESCE(a.informal_accounts, 0)  AS informal_accounts,
       CASE WHEN COALESCE(a.formal_txns, 0) > 0 AND COALESCE(a.informal_txns, 0) > 0 THEN 'both'
            WHEN COALESCE(a.formal_txns, 0) > 0   THEN 'formal only'
            WHEN COALESCE(a.informal_txns, 0) > 0 THEN 'informal only'
            ELSE 'neither' END AS financial_segment,
       a.formal_txns::numeric / NULLIF(a.formal_txns + a.informal_txns, 0) AS formal_finance_ratio,
       m.n_cash::numeric    / NULLIF(m.n_mode, 0) AS cash_dependency,
       m.n_digital::numeric / NULLIF(m.n_mode, 0) AS digital_share,
       a.first_formal_month
FROM warehouse.dim_household h
LEFT JOIN agg a   ON a.household_key = h.household_key
LEFT JOIN modes m ON m.household_key = h.household_key;

-- @mart mart_formal_adoption
-- Complement or replace? Within the same household: informal finance activity
-- per complete month BEFORE vs AFTER the household's first formal-account
-- transaction. Only households that start using formal finance after their first
-- complete month, with at least min_side_months complete months on each side
-- (the adoption month itself is left out). Association over time, not proof of
-- cause: other things change too.
CREATE TABLE marts.mart_formal_adoption AS
WITH adopters AS (
    SELECT i.household_key, i.hh_id, i.first_formal_month
    FROM marts.mart_household_inclusion i WHERE i.first_formal_month IS NOT NULL
), first_complete AS (
    SELECT household_key, MIN(year_month) AS first_complete_month
    FROM marts.mart_household_month WHERE is_complete_month GROUP BY household_key
), informal AS (
    SELECT f.household_key, d.year_month, COUNT(*) AS informal_txns, SUM(f.value_kes) AS informal_kes
    FROM warehouse.fact_transaction f
    JOIN warehouse.dim_account a   ON a.account_key = f.account_key
    JOIN warehouse.dim_trx_type t  ON t.trx_type_key = f.trx_type_key
    JOIN warehouse.dim_date d      ON d.date_key = f.date_key
    WHERE t.class_code = 'findev' AND a.formality = 'Informal'
    GROUP BY f.household_key, d.year_month
), months AS (
    SELECT hm.household_key, hm.year_month, ad.hh_id, ad.first_formal_month,
           COALESCE(i.informal_txns, 0) AS informal_txns, COALESCE(i.informal_kes, 0) AS informal_kes
    FROM marts.mart_household_month hm
    JOIN adopters ad       ON ad.household_key = hm.household_key
    JOIN first_complete fc ON fc.household_key = hm.household_key
    LEFT JOIN informal i   ON i.household_key = hm.household_key AND i.year_month = hm.year_month
    WHERE hm.is_complete_month AND ad.first_formal_month > fc.first_complete_month
), agg AS (
    SELECT household_key, hh_id, first_formal_month,
           COUNT(*) FILTER (WHERE year_month < first_formal_month) AS months_before,
           COUNT(*) FILTER (WHERE year_month > first_formal_month) AS months_after,
           AVG(informal_txns) FILTER (WHERE year_month < first_formal_month) AS informal_txns_per_month_before,
           AVG(informal_txns) FILTER (WHERE year_month > first_formal_month) AS informal_txns_per_month_after,
           AVG(informal_kes)  FILTER (WHERE year_month < first_formal_month) AS informal_kes_per_month_before,
           AVG(informal_kes)  FILTER (WHERE year_month > first_formal_month) AS informal_kes_per_month_after
    FROM months GROUP BY household_key, hh_id, first_formal_month
)
SELECT a.*,
       a.informal_txns_per_month_after - a.informal_txns_per_month_before AS change_txns_per_month,
       a.informal_kes_per_month_after  - a.informal_kes_per_month_before  AS change_kes_per_month,
       CASE WHEN a.informal_kes_per_month_after < a.informal_kes_per_month_before THEN 'informal use fell'
            WHEN a.informal_kes_per_month_after > a.informal_kes_per_month_before THEN 'informal use rose'
            ELSE 'no change' END AS direction
FROM agg a
WHERE a.months_before >= 2 AND a.months_after >= 2;


-- @mart mart_instrument_usage
-- Most commonly used financial instruments, ranked by NUMBER OF HOUSEHOLDS (not
-- transaction count). level 'family' groups types into families such as "Formal
-- loan"; level 'type' is the specific product. Households are distinct counts, so
-- do not add them up across rows.
CREATE TABLE marts.mart_instrument_usage AS
WITH base AS (
    SELECT f.household_key, COALESCE(t.instrument_group, 'other') AS instrument_group,
           t.family_desc, t.type_desc, f.value_kes
    FROM warehouse.fact_transaction f
    JOIN warehouse.dim_trx_type t ON t.trx_type_key = f.trx_type_key
    WHERE t.class_code = 'findev'
), n AS (
    SELECT COUNT(*)::numeric AS n_households FROM warehouse.dim_household
), fam AS (
    SELECT 'family' AS level, instrument_group, family_desc, NULL::text AS type_desc,
           COUNT(DISTINCT household_key) AS households, COUNT(*) AS txns, SUM(value_kes) AS total_kes
    FROM base GROUP BY instrument_group, family_desc
), typ AS (
    SELECT 'type' AS level, instrument_group, family_desc, type_desc,
           COUNT(DISTINCT household_key) AS households, COUNT(*) AS txns, SUM(value_kes) AS total_kes
    FROM base GROUP BY instrument_group, family_desc, type_desc
)
SELECT u.*, ROUND(100.0 * u.households / NULLIF(n.n_households, 0), 1) AS pct_households,
       RANK() OVER (PARTITION BY u.level ORDER BY u.households DESC, u.txns DESC) AS rank_in_level
FROM (SELECT * FROM fam UNION ALL SELECT * FROM typ) u
CROSS JOIN n;

-- @mart mart_mode_month
-- Is mobile money doing the heavy lifting? Transactions by payment mode, by
-- month and by the household's financial segment (does mobile money substitute
-- for bank access?).
CREATE TABLE marts.mart_mode_month AS
SELECT d.year_month, i.financial_segment,
       COALESCE(f.mode_desc, 'Unknown') AS mode_desc,
       BOOL_OR(f.is_cash)    AS is_cash,
       BOOL_OR(f.is_digital) AS is_digital,
       COUNT(*)                         AS txns,
       COUNT(DISTINCT f.household_key)  AS households,
       SUM(f.value_kes)                 AS value_kes
FROM warehouse.fact_transaction f
JOIN warehouse.dim_date d ON d.date_key = f.date_key
JOIN marts.mart_household_inclusion i ON i.household_key = f.household_key
GROUP BY d.year_month, i.financial_segment, COALESCE(f.mode_desc, 'Unknown');

-- @mart mart_category_month
-- Where does the money come from and where does it go? Income by source group
-- and expenditure by family, by month, with each category's share of its month.
CREATE TABLE marts.mart_category_month AS
WITH c AS (
    SELECT d.year_month, d.season,
           CASE WHEN f.flow_category = 'operating_income' THEN 'income' ELSE 'expenditure' END AS flow_type,
           CASE WHEN f.flow_category = 'operating_income'
                THEN COALESCE(t.income_source_group, 'other') ELSE t.family_desc END AS category,
           SUM(f.value_kes) AS kes,
           COUNT(DISTINCT f.household_key) AS households,
           COUNT(*) AS txns
    FROM warehouse.fact_transaction f
    JOIN warehouse.dim_date d     ON d.date_key = f.date_key
    JOIN warehouse.dim_trx_type t ON t.trx_type_key = f.trx_type_key
    WHERE f.flow_category IN ('operating_income', 'operating_expense')
    GROUP BY 1, 2, 3, 4
)
SELECT c.*, ROUND(100.0 * c.kes / NULLIF(SUM(c.kes) OVER (PARTITION BY c.year_month, c.flow_type), 0), 1)
              AS share_of_month_pct
FROM c;

-- @mart mart_monthly_totals
-- How seasonal is the economy? Complete months only, so stub months do not look
-- like bad months. One row per calendar month across all households.
CREATE TABLE marts.mart_monthly_totals AS
WITH m AS (
    SELECT year_month, MIN(season) AS season, COUNT(*) AS households,
           SUM(income_kes) AS income_kes, SUM(expenditure_kes + income_cost_kes) AS outflow_kes,
           SUM(operating_net_kes) AS net_kes,
           AVG(income_kes) AS avg_income_per_household_kes,
           AVG(expenditure_kes + income_cost_kes) AS avg_outflow_per_household_kes,
           AVG(operating_net_kes) AS avg_net_per_household_kes,
           PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY operating_net_kes) AS median_net_per_household_kes,
           COUNT(*) FILTER (WHERE is_negative_month) AS households_negative,
           SUM(borrowing_kes) AS borrowing_kes, SUM(savings_deposit_kes) AS savings_deposit_kes
    FROM marts.mart_household_month
    WHERE is_complete_month
    GROUP BY year_month
)
SELECT m.*, ROUND(100.0 * m.households_negative / NULLIF(m.households, 0), 1) AS pct_households_negative,
       RANK() OVER (ORDER BY m.households_negative::numeric / NULLIF(m.households, 0) DESC) AS hardest_month_rank
FROM m;

-- @mart mart_household_profile
-- One row per household: the clustering features and the segment labels. The
-- single table to join other marts to.
CREATE TABLE marts.mart_household_profile AS
WITH tot AS (
    SELECT household_key,
           SUM(income_kes) AS total_income_kes, SUM(expenditure_kes) AS total_expenditure_kes,
           SUM(income_cost_kes) AS total_income_cost_kes,
           SUM(savings_deposit_kes) AS total_savings_deposit_kes,
           SUM(borrowing_kes) AS total_borrowing_kes,
           SUM(borrowing_formal_kes) AS total_borrowing_formal_kes
    FROM marts.mart_household_month GROUP BY household_key
), src AS (
    SELECT f.household_key, COALESCE(t.income_source_group, 'other') AS grp, SUM(f.value_kes) AS kes
    FROM warehouse.fact_transaction f
    JOIN warehouse.dim_trx_type t ON t.trx_type_key = f.trx_type_key
    WHERE f.flow_category = 'operating_income'
    GROUP BY f.household_key, COALESCE(t.income_source_group, 'other')
), inc AS (
    SELECT household_key,
           COUNT(*) FILTER (WHERE kes > 0) AS income_diversity,
           SUM(kes) FILTER (WHERE grp = 'agriculture') AS agriculture_income_kes,
           (ARRAY_AGG(grp ORDER BY kes DESC, grp))[1] AS dominant_income_source
    FROM src GROUP BY household_key
), acc AS (
    SELECT f.household_key, COUNT(DISTINCT f.account_key) AS account_diversity
    FROM warehouse.fact_transaction f
    JOIN warehouse.dim_trx_type t ON t.trx_type_key = f.trx_type_key
    WHERE t.class_code = 'findev'
    GROUP BY f.household_key
), base AS (
    SELECT r.household_key, r.hh_id, r.months_observed, r.complete_months, r.enough_months,
           COALESCE(tot.total_income_kes, 0)           AS total_income_kes,
           COALESCE(tot.total_expenditure_kes, 0)      AS total_expenditure_kes,
           COALESCE(tot.total_savings_deposit_kes, 0)  AS total_savings_deposit_kes,
           COALESCE(tot.total_borrowing_kes, 0)        AS total_borrowing_kes,
           r.avg_monthly_income_kes, r.avg_monthly_expenditure_kes,
           r.avg_monthly_income_cost_kes, r.avg_monthly_outflow_kes, r.avg_monthly_net_kes,
           COALESCE(inc.income_diversity, 0)           AS income_diversity,
           inc.dominant_income_source,
           ROUND(COALESCE(inc.agriculture_income_kes, 0) / NULLIF(tot.total_income_kes, 0), 4) AS agricultural_dependency,
           ROUND(tot.total_savings_deposit_kes / NULLIF(tot.total_income_kes, 0), 4) AS savings_ratio,
           ROUND(tot.total_borrowing_kes / NULLIF(tot.total_income_kes, 0), 4)       AS borrowing_ratio,
           ROUND(tot.total_borrowing_formal_kes / NULLIF(tot.total_borrowing_kes, 0), 4) AS formal_borrowing_share,
           ROUND(r.share_months_with_borrowing, 4)     AS share_months_with_borrowing,
           ROUND(i.formal_finance_ratio, 4)            AS formal_finance_ratio,
           i.financial_segment,
           ROUND(i.cash_dependency, 4)                 AS cash_dependency,
           ROUND(i.digital_share, 4)                   AS digital_share,
           ROUND(r.income_volatility, 4)               AS income_volatility,
           ROUND(r.expenditure_volatility, 4)          AS expenditure_volatility,
           COALESCE(acc.account_diversity, 0)          AS account_diversity,
           ROUND(r.negative_month_share, 4)            AS negative_month_share,
           r.max_negative_streak, r.persistent_by_share, r.persistent_by_streak,
           r.persistent_either, r.persistent_both, r.vulnerability_tier,
           (r.enough_months AND COALESCE(r.avg_monthly_income_kes, 0) > 0) AS eligible_for_comparison
    FROM marts.mart_household_resilience r
    LEFT JOIN tot ON tot.household_key = r.household_key
    LEFT JOIN inc ON inc.household_key = r.household_key
    LEFT JOIN acc ON acc.household_key = r.household_key
    LEFT JOIN marts.mart_household_inclusion i ON i.household_key = r.household_key
)
SELECT b.*,
       CASE WHEN b.eligible_for_comparison
            THEN NTILE(3) OVER (PARTITION BY b.eligible_for_comparison ORDER BY b.avg_monthly_income_kes) END AS income_tercile,
       CASE WHEN b.eligible_for_comparison
            THEN NTILE(5) OVER (PARTITION BY b.eligible_for_comparison ORDER BY b.avg_monthly_income_kes) END AS income_quintile,
       CASE WHEN b.income_diversity >= 3 THEN '3+' ELSE b.income_diversity::text END AS income_source_band
FROM base b;

-- @mart mart_diversity_vs_shortage
-- Are households with more income sources less exposed to liquidity shortage?
-- Compared WITHIN income terciles, because richer households are both more
-- diversified and less short of cash. An association, not a cause.
CREATE TABLE marts.mart_diversity_vs_shortage AS
SELECT income_tercile, income_source_band,
       COUNT(*) AS households,
       ROUND(AVG(negative_month_share), 3) AS avg_negative_month_share,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY negative_month_share)::numeric, 3) AS median_negative_month_share,
       ROUND(100.0 * COUNT(*) FILTER (WHERE persistent_either) / COUNT(*), 1) AS pct_persistent
FROM marts.mart_household_profile
WHERE eligible_for_comparison
GROUP BY income_tercile, income_source_band;


-- @mart mart_savings_by_income_quintile
-- Are savings contributions associated with income levels? Medians, because
-- income is heavily skewed. The rank correlation is in mart_associations.
CREATE TABLE marts.mart_savings_by_income_quintile AS
SELECT income_quintile,
       COUNT(*) AS households,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_monthly_income_kes)::numeric, 0) AS median_monthly_income_kes,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_savings_deposit_kes)::numeric, 0) AS median_total_savings_deposit_kes,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY savings_ratio)::numeric, 3) AS median_savings_ratio,
       ROUND(100.0 * COUNT(*) FILTER (WHERE total_savings_deposit_kes > 0) / COUNT(*), 1) AS pct_households_saving
FROM marts.mart_household_profile
WHERE eligible_for_comparison
GROUP BY income_quintile;


-- @mart mart_savings_vs_borrowing
-- Do households with savings rely less on borrowing? Within income terciles.
-- The reverse is also plausible (borrowers save to repay), so read it as an
-- association. savers are split at the median savings ratio of households that save.
CREATE TABLE marts.mart_savings_vs_borrowing AS
WITH cut AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY savings_ratio) AS median_saver_ratio
    FROM marts.mart_household_profile WHERE eligible_for_comparison AND savings_ratio > 0
), g AS (
    SELECT p.*, CASE WHEN COALESCE(p.total_savings_deposit_kes, 0) = 0 THEN '1 no savings deposits'
                     WHEN p.savings_ratio < c.median_saver_ratio THEN '2 lower savings'
                     ELSE '3 higher savings' END AS savings_group
    FROM marts.mart_household_profile p CROSS JOIN cut c
    WHERE p.eligible_for_comparison
)
SELECT income_tercile, savings_group, COUNT(*) AS households,
       ROUND(100.0 * COUNT(*) FILTER (WHERE total_borrowing_kes > 0) / COUNT(*), 1) AS pct_households_borrowing,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY borrowing_ratio)::numeric, 3) AS median_borrowing_ratio,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY share_months_with_borrowing)::numeric, 3) AS median_share_months_borrowing
FROM g GROUP BY income_tercile, savings_group;

-- @mart mart_associations
-- Spearman rank correlations between household measures (ties get average
-- ranks). Rank correlation because income and ratios are heavily skewed. These
-- are associations; none of them shows cause and effect. "reliable" = at least 30
-- households.
CREATE TABLE marts.mart_associations AS
WITH elig AS (
    SELECT * FROM marts.mart_household_profile WHERE eligible_for_comparison
), pairs AS (
    SELECT 'monthly income vs savings ratio' AS pair,
           'Are savings contributions associated with income levels?' AS question, x, y
    FROM (SELECT avg_monthly_income_kes AS x, savings_ratio AS y FROM elig WHERE savings_ratio IS NOT NULL) a
    UNION ALL
    SELECT 'savings ratio vs borrowing ratio', 'Do households with savings rely less on borrowing?', x, y
    FROM (SELECT savings_ratio AS x, borrowing_ratio AS y FROM elig
          WHERE savings_ratio IS NOT NULL AND borrowing_ratio IS NOT NULL) b
    UNION ALL
    SELECT 'income sources vs share of negative months', 'Are diversified households less exposed to shortage?', x, y
    FROM (SELECT income_diversity::numeric AS x, negative_month_share AS y FROM elig) c
    UNION ALL
    SELECT 'monthly income vs share of negative months', 'Are richer households less often short of cash?', x, y
    FROM (SELECT avg_monthly_income_kes AS x, negative_month_share AS y FROM elig) d
    UNION ALL
    SELECT 'agricultural dependency vs share of negative months', 'Is dependence on farm income linked to shortage?', x, y
    FROM (SELECT agricultural_dependency AS x, negative_month_share AS y FROM elig
          WHERE agricultural_dependency IS NOT NULL) e
    UNION ALL
    SELECT 'income volatility vs borrowing ratio', 'Do households with unstable income borrow more?', x, y
    FROM (SELECT income_volatility AS x, borrowing_ratio AS y FROM elig
          WHERE income_volatility IS NOT NULL AND borrowing_ratio IS NOT NULL) f
), ranked AS (
    SELECT pair, question,
           RANK() OVER (PARTITION BY pair ORDER BY x) + (COUNT(*) OVER (PARTITION BY pair, x) - 1) / 2.0 AS rx,
           RANK() OVER (PARTITION BY pair ORDER BY y) + (COUNT(*) OVER (PARTITION BY pair, y) - 1) / 2.0 AS ry
    FROM pairs
)
SELECT pair, question, COUNT(*) AS households,
       ROUND(CORR(rx, ry)::numeric, 3) AS spearman_rho,
       CASE WHEN ABS(CORR(rx, ry)) < 0.1 THEN 'negligible'
            WHEN ABS(CORR(rx, ry)) < 0.3 THEN 'weak'
            WHEN ABS(CORR(rx, ry)) < 0.5 THEN 'moderate'
            ELSE 'strong' END AS strength,
       CASE WHEN CORR(rx, ry) > 0 THEN 'positive' WHEN CORR(rx, ry) < 0 THEN 'negative' END AS direction,
       COUNT(*) >= 30 AS reliable
FROM ranked GROUP BY pair, question;


-- @mart mart_borrowing_low_vs_high
-- Does borrowing increase in low-income periods? A within-household comparison,
-- which removes differences between households. A household's low-income months
-- are complete months with income below its own median, high-income months
-- above it (months at the median are left out). Needs at least min_band_months
-- of each.
CREATE TABLE marts.mart_borrowing_low_vs_high AS
WITH cm AS (
    SELECT household_key, hh_id, income_kes, borrowing_kes
    FROM marts.mart_household_month WHERE is_complete_month
), med AS (
    SELECT household_key, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY income_kes) AS median_income_kes
    FROM cm GROUP BY household_key
), tagged AS (
    SELECT cm.*, CASE WHEN cm.income_kes < med.median_income_kes THEN 'low'
                      WHEN cm.income_kes > med.median_income_kes THEN 'high' END AS band
    FROM cm JOIN med USING (household_key)
), agg AS (
    SELECT household_key, hh_id,
           COUNT(*) FILTER (WHERE band = 'low')  AS low_income_months,
           COUNT(*) FILTER (WHERE band = 'high') AS high_income_months,
           AVG(borrowing_kes) FILTER (WHERE band = 'low')  AS borrowing_per_month_low_kes,
           AVG(borrowing_kes) FILTER (WHERE band = 'high') AS borrowing_per_month_high_kes,
           AVG((borrowing_kes > 0)::int) FILTER (WHERE band = 'low')  AS share_months_borrowing_low,
           AVG((borrowing_kes > 0)::int) FILTER (WHERE band = 'high') AS share_months_borrowing_high
    FROM tagged GROUP BY household_key, hh_id
)
SELECT a.*, a.borrowing_per_month_low_kes - a.borrowing_per_month_high_kes AS difference_kes,
       CASE WHEN a.borrowing_per_month_low_kes > a.borrowing_per_month_high_kes THEN 'more in low-income months'
            WHEN a.borrowing_per_month_low_kes < a.borrowing_per_month_high_kes THEN 'more in high-income months'
            ELSE 'same' END AS result
FROM agg a
WHERE a.low_income_months >= 2 AND a.high_income_months >= 2;


-- @mart mart_borrowing_low_vs_high_summary
-- One row: how many households borrow more in their low-income months. Households
-- that never borrow count as "same" and are reported, because they dilute the share.
CREATE TABLE marts.mart_borrowing_low_vs_high_summary AS
SELECT COUNT(*) AS households_compared,
       COUNT(*) FILTER (WHERE result = 'more in low-income months')  AS more_in_low,
       COUNT(*) FILTER (WHERE result = 'more in high-income months') AS more_in_high,
       COUNT(*) FILTER (WHERE result = 'same')                       AS same,
       ROUND(100.0 * COUNT(*) FILTER (WHERE result = 'more in low-income months') / NULLIF(COUNT(*), 0), 1) AS pct_more_in_low,
       ROUND(100.0 * COUNT(*) FILTER (WHERE result = 'more in high-income months') / NULLIF(COUNT(*), 0), 1) AS pct_more_in_high,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY difference_kes)
             FILTER (WHERE result <> 'same')::numeric, 0) AS median_difference_kes_among_borrowers
FROM marts.mart_borrowing_low_vs_high;


-- @mart mart_borrowing_purpose
-- What do households borrow for? The loan type says what the money was for
-- (school fees, business, emergencies). Repayments are not included.
CREATE TABLE marts.mart_borrowing_purpose AS
SELECT CASE t.instrument_group WHEN 'formal_loan' THEN 'formal'
                               WHEN 'employer_credit' THEN 'employer'
                               WHEN 'informal_credit' THEN 'informal'
                               WHEN 'informal_group' THEN 'informal'
                               WHEN 'informal_p2p' THEN 'informal'
                               ELSE 'other' END AS borrowing_type,
       t.family_desc, t.type_desc,
       COUNT(DISTINCT f.household_key) AS households,
       COUNT(*) AS loans,
       SUM(f.value_kes) AS total_kes,
       ROUND(100.0 * SUM(f.value_kes) / NULLIF(SUM(SUM(f.value_kes)) OVER (), 0), 1) AS share_of_borrowing_pct,
       RANK() OVER (ORDER BY SUM(f.value_kes) DESC) AS rank_by_kes
FROM warehouse.fact_transaction f
JOIN warehouse.dim_trx_type t ON t.trx_type_key = f.trx_type_key
WHERE f.flow_category = 'borrowing'
GROUP BY 1, t.family_desc, t.type_desc;


-- @mart mart_income_sources
-- Where does the money come from, and how exposed is the area to a bad harvest?
-- Share of all income by source, and how many households depend mainly on each.
CREATE TABLE marts.mart_income_sources AS
WITH s AS (
    SELECT COALESCE(t.income_source_group, 'other') AS income_source_group,
           SUM(f.value_kes) AS income_kes,
           COUNT(DISTINCT f.household_key) AS households_with_income
    FROM warehouse.fact_transaction f
    JOIN warehouse.dim_trx_type t ON t.trx_type_key = f.trx_type_key
    WHERE f.flow_category = 'operating_income'
    GROUP BY COALESCE(t.income_source_group, 'other')
), dom AS (
    SELECT dominant_income_source AS income_source_group, COUNT(*) AS households_mainly_dependent
    FROM marts.mart_household_profile GROUP BY dominant_income_source
)
SELECT s.income_source_group, s.income_kes,
       ROUND(100.0 * s.income_kes / NULLIF(SUM(s.income_kes) OVER (), 0), 1) AS share_of_income_pct,
       s.households_with_income,
       COALESCE(d.households_mainly_dependent, 0) AS households_mainly_dependent
FROM s LEFT JOIN dom d USING (income_source_group);


-- @mart mart_vulnerability_by_group
-- Who is vulnerable and what do they have in common? Share of households with
-- persistent negative net cash flow (either rule) within each group.
CREATE TABLE marts.mart_vulnerability_by_group AS
WITH p AS (SELECT * FROM marts.mart_household_profile WHERE enough_months)
SELECT 'dominant income source' AS grouping, COALESCE(dominant_income_source, 'none') AS value,
       COUNT(*) AS households, COUNT(*) FILTER (WHERE persistent_either) AS persistent_households,
       ROUND(100.0 * COUNT(*) FILTER (WHERE persistent_either) / COUNT(*), 1) AS pct_persistent
FROM p GROUP BY 2
UNION ALL
SELECT 'financial segment', COALESCE(financial_segment, 'neither'), COUNT(*),
       COUNT(*) FILTER (WHERE persistent_either),
       ROUND(100.0 * COUNT(*) FILTER (WHERE persistent_either) / COUNT(*), 1)
FROM p GROUP BY 2
UNION ALL
SELECT 'income tercile (1 = lowest)', COALESCE(income_tercile::text, 'no income'), COUNT(*),
       COUNT(*) FILTER (WHERE persistent_either),
       ROUND(100.0 * COUNT(*) FILTER (WHERE persistent_either) / COUNT(*), 1)
FROM p GROUP BY 2
UNION ALL
SELECT 'number of income sources', income_source_band, COUNT(*),
       COUNT(*) FILTER (WHERE persistent_either),
       ROUND(100.0 * COUNT(*) FILTER (WHERE persistent_either) / COUNT(*), 1)
FROM p GROUP BY 2;


-- @mart mart_shock_simulation
-- What would a shock look like? For each household and each simulated income
-- drop: income falls by that percent, outflows (expenditure + income costs) stay
-- the same, and the monthly shortfall is covered from liquid assets. Liquid
-- assets = latest closing balance of each asset account labelled "More liquid",
-- plus the latest cash-on-hand reading. Averages are over complete months.
CREATE TABLE marts.mart_shock_simulation AS
WITH acct AS (
    SELECT DISTINCT ON (b.account_key) b.household_key, b.balance_kes
    FROM warehouse.fact_account_balance b
    JOIN warehouse.dim_account a ON a.account_key = b.account_key
    WHERE b.snapshot_type = 'close' AND a.bsheet_designation = 'Asset' AND a.liquidity = 'More liquid'
    ORDER BY b.account_key, b.date_key DESC, b.source_row_id DESC
), cash AS (
    SELECT DISTINCT ON (household_key) household_key, balance_kes
    FROM warehouse.fact_account_balance
    WHERE snapshot_type = 'cash_on_hand'
    ORDER BY household_key, date_key DESC, source_row_id DESC
), acct_total AS (
    SELECT household_key, SUM(balance_kes) AS balance_kes FROM acct GROUP BY household_key
), liquid AS (
    -- joins, not correlated subqueries: those were re-run for every row and column
    SELECT h.household_key,
           COALESCE(t.balance_kes, 0) + COALESCE(c.balance_kes, 0) AS liquid_assets_kes
    FROM warehouse.dim_household h
    LEFT JOIN acct_total t ON t.household_key = h.household_key
    LEFT JOIN cash c       ON c.household_key = h.household_key
), sim AS (
    SELECT p.household_key, p.hh_id, s.shock_pct,
           p.avg_monthly_income_kes, p.avg_monthly_outflow_kes,
           p.avg_monthly_income_kes * (1 - s.shock_pct / 100.0) AS shocked_income_kes,
           GREATEST(p.avg_monthly_outflow_kes - p.avg_monthly_income_kes * (1 - s.shock_pct / 100.0), 0) AS monthly_shortfall_kes,
           l.liquid_assets_kes
    FROM marts.mart_household_profile p
    CROSS JOIN (VALUES {shock_values}) AS s(shock_pct)
    JOIN liquid l ON l.household_key = p.household_key
    WHERE p.enough_months
)
SELECT sim.*,
       CASE WHEN sim.monthly_shortfall_kes > 0 THEN sim.liquid_assets_kes / sim.monthly_shortfall_kes END AS months_covered,
       CASE WHEN sim.monthly_shortfall_kes = 0 THEN 'no shortfall'
            WHEN sim.liquid_assets_kes / sim.monthly_shortfall_kes >= 6 THEN 'covers 6+ months'
            WHEN sim.liquid_assets_kes / sim.monthly_shortfall_kes >= 3 THEN 'covers 3 to 6 months'
            WHEN sim.liquid_assets_kes / sim.monthly_shortfall_kes >= 1 THEN 'covers 1 to 3 months'
            ELSE 'covers under 1 month' END AS coverage_band
FROM sim;


-- @mart mart_shock_summary
-- Households per coverage band for each simulated income drop.
CREATE TABLE marts.mart_shock_summary AS
SELECT shock_pct, coverage_band, COUNT(*) AS households,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY shock_pct), 1) AS pct_households
FROM marts.mart_shock_simulation
GROUP BY shock_pct, coverage_band;


-- @mart mart_kpis
-- One row of headline numbers for the top of a dashboard.
CREATE TABLE marts.mart_kpis AS
WITH p AS (SELECT * FROM marts.mart_household_profile),
e AS (SELECT * FROM p WHERE enough_months),
s25 AS (SELECT * FROM marts.mart_shock_simulation WHERE shock_pct = 25)
SELECT (SELECT COUNT(*) FROM p) AS households,
       (SELECT COUNT(*) FROM e) AS households_with_enough_months,
       ROUND(100.0 * COUNT(*) FILTER (WHERE e.persistent_either) / NULLIF(COUNT(*), 0), 1)  AS pct_persistent_either_rule,
       ROUND(100.0 * COUNT(*) FILTER (WHERE e.persistent_by_share) / NULLIF(COUNT(*), 0), 1) AS pct_persistent_by_share,
       ROUND(100.0 * COUNT(*) FILTER (WHERE e.persistent_by_streak) / NULLIF(COUNT(*), 0), 1) AS pct_persistent_by_streak,
       ROUND(100.0 * COUNT(*) FILTER (WHERE e.financial_segment IN ('formal only', 'both')) / NULLIF(COUNT(*), 0), 1) AS pct_using_formal,
       ROUND(100.0 * COUNT(*) FILTER (WHERE e.financial_segment = 'informal only') / NULLIF(COUNT(*), 0), 1) AS pct_informal_only,
       ROUND(100.0 * COUNT(*) FILTER (WHERE e.financial_segment = 'both') / NULLIF(COUNT(*), 0), 1) AS pct_both_formal_and_informal,
       ROUND(100.0 * COUNT(*) FILTER (WHERE e.digital_share > 0) / NULLIF(COUNT(*), 0), 1) AS pct_using_mobile_money,
       ROUND(100.0 * COUNT(*) FILTER (WHERE e.agricultural_dependency >= 0.5) / NULLIF(COUNT(*), 0), 1) AS pct_mainly_farm_income,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.negative_month_share)::numeric, 3) AS median_negative_month_share,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.savings_ratio)::numeric, 3) AS median_savings_ratio,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.borrowing_ratio)::numeric, 3) AS median_borrowing_ratio,
       (SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE coverage_band IN ('no shortfall', 'covers 3 to 6 months', 'covers 6+ months'))
                     / NULLIF(COUNT(*), 0), 1) FROM s25) AS pct_covering_3_months_if_income_falls_25
FROM e;


-- @mart mart_data_quality
-- What did the pipeline reject or flag in its latest run? For analysts, so that
-- numbers can be read with the data problems in view.
CREATE TABLE marts.mart_data_quality AS
SELECT 'rejected: ' || reject_reason AS check_name, COUNT(*) AS row_count, SUM(value_kes) AS kes
FROM warehouse.etl_rejects
WHERE run_id = (SELECT run_id FROM warehouse.etl_rejects ORDER BY reject_id DESC LIMIT 1)
GROUP BY reject_reason
UNION ALL
SELECT 'flagged: ' || flag, COUNT(*), SUM(value_kes)
FROM warehouse.fact_transaction, UNNEST(STRING_TO_ARRAY(dq_flags, ';')) AS flag
WHERE dq_flags <> ''
GROUP BY flag;
-- tests/assert_segment_completeness.sql
-- セグメント別集計の合計が「All」セグメントと一致することを検証

WITH segment_totals AS (
    SELECT
        time_grain,
        CAST(period_start AS DATE) AS period_start,
        category,
        SUM(total_sales) AS segment_total
    FROM {{ ref('fct_sales_multidimensional') }}
    WHERE customer_segment != 'All'
    GROUP BY time_grain, period_start, category
),

all_totals AS (
    SELECT
        time_grain,
        CAST(period_start AS DATE) AS period_start,
        category,
        total_sales AS all_total
    FROM {{ ref('fct_sales_multidimensional') }}
    WHERE customer_segment = 'All'
)

SELECT
    s.time_grain,
    s.period_start,
    s.category,
    s.segment_total,
    a.all_total,
    ABS(s.segment_total - a.all_total) AS difference
FROM segment_totals s
JOIN all_totals a
    ON s.time_grain = a.time_grain
    AND s.period_start = a.period_start
    AND s.category = a.category
WHERE ABS(s.segment_total - a.all_total) / a.all_total > 0.01
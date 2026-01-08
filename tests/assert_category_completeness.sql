-- tests/assert_category_completeness.sql
-- カテゴリ別集計の合計が「All」カテゴリと一致することを検証

WITH category_totals AS (
    SELECT
        time_grain,
        CAST(period_start AS DATE) AS period_start,
        customer_segment,
        SUM(total_sales) AS category_total
    FROM {{ ref('fct_sales_multidimensional') }}
    WHERE category != 'All'
    GROUP BY time_grain, period_start, customer_segment
),

all_totals AS (
    SELECT
        time_grain,
        CAST(period_start AS DATE) AS period_start,
        customer_segment,
        total_sales AS all_total
    FROM {{ ref('fct_sales_multidimensional') }}
    WHERE category = 'All'
)

SELECT
    c.time_grain,
    c.period_start,
    c.customer_segment,
    c.category_total,
    a.all_total,
    ABS(c.category_total - a.all_total) AS difference
FROM category_totals c
JOIN all_totals a
    ON c.time_grain = a.time_grain
    AND c.period_start = a.period_start
    AND c.customer_segment = a.customer_segment
WHERE ABS(c.category_total - a.all_total) / a.all_total > 0.01

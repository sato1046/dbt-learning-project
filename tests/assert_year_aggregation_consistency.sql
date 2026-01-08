-- tests/assert_year_aggregation_consistency.sql
-- 年次集計が月次集計の合計と一致することを検証

WITH yearly_totals AS (
    SELECT
        category,
        customer_segment,
        SUM(total_sales) AS yearly_total
    FROM {{ ref('fct_sales_multidimensional') }}
    WHERE time_grain = 'Year'
    GROUP BY category, customer_segment
),

monthly_totals AS (
    SELECT
        category,
        customer_segment,
        SUM(total_sales) AS monthly_total
    FROM {{ ref('fct_sales_multidimensional') }}
    WHERE time_grain = 'Month'
    GROUP BY category, customer_segment
)

SELECT
    y.category,
    y.customer_segment,
    y.yearly_total,
    m.monthly_total,
    ABS(y.yearly_total - m.monthly_total) AS difference
FROM yearly_totals y
JOIN monthly_totals m
    ON y.category = m.category
    AND y.customer_segment = m.customer_segment
WHERE ABS(y.yearly_total - m.monthly_total) / y.yearly_total > 0.01
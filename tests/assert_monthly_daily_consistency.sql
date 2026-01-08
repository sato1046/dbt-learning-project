-- tests/assert_monthly_daily_consistency.sql
-- 月次集計の合計が日次集計の合計と一致することを検証

WITH monthly_totals AS (
    SELECT
        category,
        customer_segment,
        SUM(total_sales) AS monthly_total
    FROM {{ ref('fct_sales_multidimensional') }}
    WHERE time_grain = 'Month'
    GROUP BY category, customer_segment
),

daily_totals AS (
    SELECT
        category,
        customer_segment,
        SUM(total_sales) AS daily_total
    FROM {{ ref('fct_sales_multidimensional') }}
    WHERE time_grain = 'Day'
    GROUP BY category, customer_segment
),

comparison AS (
    SELECT
        m.category,
        m.customer_segment,
        m.monthly_total,
        d.daily_total,
        ABS(m.monthly_total - d.daily_total) AS difference,
        ABS(m.monthly_total - d.daily_total) / m.monthly_total AS difference_pct
    FROM monthly_totals m
    JOIN daily_totals d
        ON m.category = d.category
        AND m.customer_segment = d.customer_segment
)

SELECT *
FROM comparison
WHERE difference_pct > 0.01  -- 1%以上の差異があれば失敗
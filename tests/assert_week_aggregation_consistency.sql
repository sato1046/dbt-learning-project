-- tests/assert_week_aggregation_consistency.sql
-- 週次集計が日次集計の合計と一致することを検証

WITH weekly_totals AS (
    SELECT
        category,
        customer_segment,
        SUM(total_sales) AS weekly_total
    FROM {{ ref('fct_sales_multidimensional') }}
    WHERE time_grain = 'Week'
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
)

SELECT
    w.category,
    w.customer_segment,
    w.weekly_total,
    d.daily_total,
    ABS(w.weekly_total - d.daily_total) AS difference
FROM weekly_totals w
JOIN daily_totals d
    ON w.category = d.category
    AND w.customer_segment = d.customer_segment
WHERE ABS(w.weekly_total - d.daily_total) / w.weekly_total > 0.01
-- tests/assert_positive_sales.sql
-- 売上金額が0以下のレコードがないことを検証

SELECT
    time_grain,
    period_start,
    category,
    customer_segment,
    total_sales
FROM {{ ref('fct_sales_multidimensional') }}
WHERE total_sales <= 0
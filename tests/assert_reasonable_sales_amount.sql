{{ config(severity='warn') }}

-- tests/assert_reasonable_sales_amount.sql
-- 売上金額が異常に高くないことを検証（1,000,000以上は警告）

SELECT
    time_grain,
    period_start,
    category,
    customer_segment,
    total_sales
FROM {{ ref('fct_sales_multidimensional') }}
WHERE total_sales > 1000000
{{ config(severity='warn') }}

-- tests/assert_zero_sales_records.sql
-- 売上ゼロのレコードを検出（警告レベル）

SELECT
    time_grain,
    period_start,
    category,
    customer_segment,
    total_sales,
    order_count,
    customer_count
FROM {{ ref('fct_sales_multidimensional') }}
WHERE total_sales = 0
  AND order_count > 0
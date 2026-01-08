{{ config(severity='warn') }}

-- tests/assert_avg_order_value_calculation.sql
-- 平均注文額が正しく計算されていることを検証

SELECT
    time_grain,
    period_start,
    category,
    customer_segment,
    avg_order_value,
    total_sales / order_count AS calculated_avg,
    ABS(avg_order_value - (total_sales / order_count)) AS difference,
    ABS(avg_order_value - (total_sales / order_count)) / avg_order_value AS difference_pct
FROM {{ ref('fct_sales_multidimensional') }}
WHERE order_count > 0  -- 0除算を回避
  AND ABS(avg_order_value - (total_sales / order_count)) / avg_order_value > 0.001  -- 0.1%の許容誤差
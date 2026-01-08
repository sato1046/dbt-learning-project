-- tests/assert_order_customer_consistency.sql
-- 注文数が顧客数以上であることを検証
-- （1人の顧客が複数注文する可能性があるため）

SELECT
    time_grain,
    period_start,
    category,
    customer_segment,
    order_count,
    customer_count
FROM {{ ref('fct_sales_multidimensional') }}
WHERE order_count < customer_count
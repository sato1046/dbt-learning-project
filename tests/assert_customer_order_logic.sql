-- tests/assert_customer_order_logic.sql  
-- 顧客数が0の場合は注文数も0であることを検証

SELECT *
FROM {{ ref('fct_sales_multidimensional') }}
WHERE customer_count = 0 AND order_count > 0
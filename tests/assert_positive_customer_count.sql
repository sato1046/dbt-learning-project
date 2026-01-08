-- tests/assert_positive_customer_count.sql
-- 顧客数が0より大きいことを検証

SELECT *
FROM {{ ref('fct_sales_multidimensional') }}
WHERE customer_count <= 0
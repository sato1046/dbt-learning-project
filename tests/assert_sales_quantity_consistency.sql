-- tests/assert_sales_quantity_consistency.sql
-- 売上がある場合は数量も存在することを検証

SELECT *
FROM {{ ref('fct_sales_multidimensional') }}
WHERE total_sales > 0 AND total_quantity = 0
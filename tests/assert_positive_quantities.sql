-- tests/assert_positive_quantities.sql
-- 数量が0より大きいことを検証

SELECT *
FROM {{ ref('fct_sales_multidimensional') }}
WHERE total_quantity <= 0
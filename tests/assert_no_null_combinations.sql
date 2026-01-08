{{ config(severity='warn') }}

-- tests/assert_no_null_combinations.sql
-- カテゴリとセグメントの特定の組み合わせをチェック

SELECT *
FROM {{ ref('fct_sales_multidimensional') }}
WHERE category IS NULL
   OR customer_segment IS NULL
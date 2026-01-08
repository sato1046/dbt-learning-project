-- tests/assert_product_sales_completeness.sql
-- 全ての商品が売上サマリーに存在することを検証

SELECT
    p.product_id,
    p.product_name
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('product_sales_summary') }} s
    ON p.product_id = s.product_id
WHERE s.product_id IS NULL
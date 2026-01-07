-- models/marts/product_sales_summary.sql
-- 商品マスタ情報サマリー

SELECT
    product_id,
    product_name,
    category,
    price,
    status
FROM {{ ref('stg_products') }}
ORDER BY category, product_name
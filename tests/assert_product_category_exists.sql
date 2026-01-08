-- tests/assert_product_category_exists.sql
-- 全ての商品に有効なカテゴリが存在することを検証

SELECT
    product_id,
    product_name,
    category
FROM {{ ref('stg_products') }}
WHERE category IS NULL
   OR TRIM(category) = ''
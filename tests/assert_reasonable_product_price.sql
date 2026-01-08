{{ config(severity='warn') }}

-- tests/assert_reasonable_product_price.sql
-- 商品価格が妥当な範囲内であることを検証

SELECT
    product_id,
    product_name,
    price
FROM {{ ref('stg_products') }}
WHERE price <= 0 OR price > 100000
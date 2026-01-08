-- tests/assert_order_items_product_fk.sql
-- order_items.product_idがproductsに存在することを検証

SELECT
    oi.order_item_id,
    oi.product_id
FROM {{ ref('stg_order_items') }} oi
LEFT JOIN {{ ref('stg_products') }} p
    ON oi.product_id = p.product_id
WHERE p.product_id IS NULL
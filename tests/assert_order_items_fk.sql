-- tests/assert_order_items_fk.sql
-- order_items.order_idがordersに存在することを検証

SELECT
    oi.order_item_id,
    oi.order_id
FROM {{ ref('stg_order_items') }} oi
LEFT JOIN {{ ref('stg_orders') }} o
    ON oi.order_id = o.order_id
WHERE o.order_id IS NULL
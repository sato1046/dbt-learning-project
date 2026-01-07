-- models/staging/stg_order_items.sql
-- 注文明細データの整形

SELECT
    id AS order_item_id,
    order_id,
    product_id,
    user_id,
    quantity,
    sale_price,
    created_at
FROM {{ source('raw_thelook', 'order_items') }}
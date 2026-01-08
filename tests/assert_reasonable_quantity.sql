{{ config(severity='warn') }}

-- tests/assert_reasonable_quantity.sql
-- 注文数量が異常に多くないことを検証（100個以上は警告）

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity
FROM {{ ref('stg_order_items') }}
WHERE quantity > 100
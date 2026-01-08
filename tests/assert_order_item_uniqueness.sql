-- tests/assert_order_item_uniqueness.sql
-- 同一注文内で同一商品が重複していないことを検証

WITH duplicates AS (
    SELECT
        order_id,
        product_id,
        COUNT(*) AS item_count
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id, product_id
    HAVING COUNT(*) > 1
)

SELECT * FROM duplicates
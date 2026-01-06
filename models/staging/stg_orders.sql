-- models/staging/stg_orders.sql

SELECT
    order_id,
    user_id,
    created_at,
    status,
    order_amount
FROM {{ source('raw_thelook', 'orders') }}
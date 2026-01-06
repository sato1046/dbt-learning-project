-- models/staging/stg_products.sql

SELECT
    id AS product_id,
    name AS product_name,
    category,
    price,
    status
FROM {{ source('raw_thelook', 'products') }}
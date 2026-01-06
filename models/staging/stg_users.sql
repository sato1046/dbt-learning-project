-- models/staging/stg_users.sql

SELECT
    id AS user_id,
    first_name,
    last_name,
    email,
    city,
    country,
    age,
    gender,
    created_at
FROM {{ source('raw_thelook', 'users') }}
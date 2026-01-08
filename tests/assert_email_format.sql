{{ config(severity='warn') }}

-- tests/assert_email_format.sql
-- メールアドレスが@を含むことを検証

SELECT
    user_id,
    email
FROM {{ ref('stg_users') }}
WHERE email NOT LIKE '%@%'
   OR email NOT LIKE '%.%'
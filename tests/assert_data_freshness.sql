{{ config(severity='warn') }}

-- tests/assert_data_freshness.sql
-- データが過去30日以内に存在することを検証（データ鮮度チェック）

WITH max_date AS (
    SELECT MAX(order_date) AS latest_date
    FROM {{ ref('stg_sales') }}
)

SELECT
    latest_date,
    CURRENT_DATE() AS today,
    DATE_DIFF(CURRENT_DATE(), latest_date, DAY) AS days_old
FROM max_date
WHERE DATE_DIFF(CURRENT_DATE(), latest_date, DAY) > 30
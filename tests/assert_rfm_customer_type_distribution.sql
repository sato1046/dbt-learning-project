{{ config(severity='warn') }}

-- tests/assert_rfm_customer_type_distribution.sql
-- 各顧客タイプに少なくとも1人は存在することを検証

WITH customer_type_counts AS (
    SELECT
        customer_type,
        COUNT(*) AS customer_count
    FROM {{ ref('int_customer_rfm') }}
    GROUP BY customer_type
)

SELECT *
FROM (
    SELECT 'New' AS customer_type
    UNION ALL SELECT 'Existing'
    UNION ALL SELECT 'Loyal'
) expected
LEFT JOIN customer_type_counts actual
    USING (customer_type)
WHERE actual.customer_count IS NULL
   OR actual.customer_count = 0
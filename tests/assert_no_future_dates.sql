{{ config(severity='warn') }}

-- tests/assert_no_future_dates.sql
-- 未来日付のレコードがないことを検証

SELECT
    time_grain,
    period_start,
    category,
    customer_segment
FROM {{ ref('fct_sales_multidimensional') }}
WHERE period_start > CURRENT_DATE()
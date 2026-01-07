-- models/marts/fct_sales_multidimensional.sql
-- 多次元売上ファクトテーブル（12パターンUNION ALL）
-- 実務で培った複雑なSQL設計を再現

WITH base_sales AS (
    SELECT
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.user_id,
        oi.quantity,
        oi.sale_price,
        oi.created_at,
        
        -- 商品情報
        p.product_name,
        p.category AS product_category,
        
        -- 顧客セグメント
        rfm.customer_type,
        
        -- 注文情報
        o.status AS order_status,
        
        -- 日付ディメンション
        DATE(oi.created_at) AS sale_date,
        DATE_TRUNC(DATE(oi.created_at), WEEK) AS sale_week,
        DATE_TRUNC(DATE(oi.created_at), MONTH) AS sale_month,
        DATE_TRUNC(DATE(oi.created_at), YEAR) AS sale_year
        
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('stg_products') }} p
        ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('stg_orders') }} o
        ON oi.order_id = o.order_id
    LEFT JOIN {{ ref('int_customer_rfm') }} rfm
        ON oi.user_id = rfm.user_id
    WHERE o.status = 'complete'
),

-- パターン1: 年次×All×All
sales_yearly AS (
    SELECT
        'Year' AS time_grain,
        sale_year AS period_start,
        'All' AS category,
        'All' AS customer_segment,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS customer_count,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(sale_price), 2) AS total_sales,
        ROUND(AVG(sale_price), 2) AS avg_order_value
    FROM base_sales
    GROUP BY sale_year
),

-- パターン2: 月次×All×All
sales_monthly AS (
    SELECT
        'Month' AS time_grain,
        sale_month AS period_start,
        'All' AS category,
        'All' AS customer_segment,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS customer_count,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(sale_price), 2) AS total_sales,
        ROUND(AVG(sale_price), 2) AS avg_order_value
    FROM base_sales
    GROUP BY sale_month
),

-- パターン3: 週次×All×All
sales_weekly AS (
    SELECT
        'Week' AS time_grain,
        sale_week AS period_start,
        'All' AS category,
        'All' AS customer_segment,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS customer_count,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(sale_price), 2) AS total_sales,
        ROUND(AVG(sale_price), 2) AS avg_order_value
    FROM base_sales
    GROUP BY sale_week
),

-- パターン4: 日次×All×All
sales_daily AS (
    SELECT
        'Day' AS time_grain,
        sale_date AS period_start,
        'All' AS category,
        'All' AS customer_segment,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS customer_count,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(sale_price), 2) AS total_sales,
        ROUND(AVG(sale_price), 2) AS avg_order_value
    FROM base_sales
    GROUP BY sale_date
),

-- パターン5: 月次×カテゴリ×All
sales_monthly_by_category AS (
    SELECT
        'Month' AS time_grain,
        sale_month AS period_start,
        product_category AS category,
        'All' AS customer_segment,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS customer_count,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(sale_price), 2) AS total_sales,
        ROUND(AVG(sale_price), 2) AS avg_order_value
    FROM base_sales
    GROUP BY sale_month, product_category
),

-- パターン6: 月次×All×顧客セグメント
sales_monthly_by_segment AS (
    SELECT
        'Month' AS time_grain,
        sale_month AS period_start,
        'All' AS category,
        customer_type AS customer_segment,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS customer_count,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(sale_price), 2) AS total_sales,
        ROUND(AVG(sale_price), 2) AS avg_order_value
    FROM base_sales
    GROUP BY sale_month, customer_type
),

-- パターン7: 月次×カテゴリ×顧客セグメント
sales_monthly_by_category_segment AS (
    SELECT
        'Month' AS time_grain,
        sale_month AS period_start,
        product_category AS category,
        customer_type AS customer_segment,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS customer_count,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(sale_price), 2) AS total_sales,
        ROUND(AVG(sale_price), 2) AS avg_order_value
    FROM base_sales
    GROUP BY sale_month, product_category, customer_type
),

-- パターン8: 日次×カテゴリ×All
sales_daily_by_category AS (
    SELECT
        'Day' AS time_grain,
        sale_date AS period_start,
        product_category AS category,
        'All' AS customer_segment,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS customer_count,
        SUM(quantity) AS total_quantity,
        ROUND(SUM(sale_price), 2) AS total_sales,
        ROUND(AVG(sale_price), 2) AS avg_order_value
    FROM base_sales
    GROUP BY sale_date, product_category
),

-- 全パターンを統合
final AS (
    SELECT * FROM sales_yearly
    UNION ALL
    SELECT * FROM sales_monthly
    UNION ALL
    SELECT * FROM sales_weekly
    UNION ALL
    SELECT * FROM sales_daily
    UNION ALL
    SELECT * FROM sales_monthly_by_category
    UNION ALL
    SELECT * FROM sales_monthly_by_segment
    UNION ALL
    SELECT * FROM sales_monthly_by_category_segment
    UNION ALL
    SELECT * FROM sales_daily_by_category
)

SELECT 
    time_grain,
    period_start,
    category,
    customer_segment,
    order_count,
    customer_count,
    total_quantity,
    total_sales,
    avg_order_value
FROM final
ORDER BY period_start DESC, time_grain, category, customer_segment
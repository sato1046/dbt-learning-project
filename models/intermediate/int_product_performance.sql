-- models/intermediate/int_product_performance.sql
-- 商品別のパフォーマンス分析

WITH product_sales AS (
    SELECT
        oi.product_id,
        p.product_name,
        p.category,
        p.price AS list_price,
        oi.order_id,
        oi.quantity,
        oi.sale_price,
        oi.created_at,
        o.user_id
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('stg_products') }} p
        ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('stg_orders') }} o
        ON oi.order_id = o.order_id
    WHERE o.status = 'complete'
),

product_metrics AS (
    SELECT
        product_id,
        product_name,
        category,
        list_price,
        
        -- 販売実績
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        SUM(quantity) AS total_quantity_sold,
        ROUND(SUM(sale_price), 2) AS total_revenue,
        
        -- 平均値
        ROUND(AVG(sale_price), 2) AS avg_sale_price,
        ROUND(AVG(quantity), 2) AS avg_quantity_per_order,
        
        -- 最初と最後の販売日
        MIN(DATE(created_at)) AS first_sale_date,
        MAX(DATE(created_at)) AS last_sale_date
        
    FROM product_sales
    GROUP BY product_id, product_name, category, list_price
),

product_rankings AS (
    SELECT
        *,
        
        -- カテゴリ内ランキング
        ROW_NUMBER() OVER (
            PARTITION BY category 
            ORDER BY total_revenue DESC
        ) AS revenue_rank_in_category,
        
        -- 全体ランキング
        ROW_NUMBER() OVER (
            ORDER BY total_revenue DESC
        ) AS revenue_rank_overall,
        
        -- 販売数量ランキング
        ROW_NUMBER() OVER (
            ORDER BY total_quantity_sold DESC
        ) AS quantity_rank_overall
        
    FROM product_metrics
)

SELECT * FROM product_rankings
ORDER BY total_revenue DESC
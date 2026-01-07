-- models/marts/product_sales_summary.sql
-- 商品別売上サマリー（マーケティング分析用）

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.user_id) AS unique_customers,
    ROUND(SUM(o.order_amount), 2) AS total_sales,
    ROUND(AVG(o.order_amount), 2) AS avg_order_amount
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('stg_orders') }} o
    ON p.product_id = o.order_id  -- 注: 実際のデータ構造に合わせて調整が必要
GROUP BY 1, 2, 3, 4
ORDER BY total_sales DESC
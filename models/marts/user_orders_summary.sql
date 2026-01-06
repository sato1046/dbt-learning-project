-- models/marts/user_orders_summary.sql
-- ユーザーごとの注文サマリー（マーケティング分析用）

SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    u.city,
    u.country,
    u.age,
    u.gender,
    COUNT(o.order_id) AS total_orders,
    SUM(CASE WHEN o.status = 'complete' THEN 1 ELSE 0 END) AS completed_orders,
    SUM(CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    ROUND(SUM(o.order_amount), 2) AS total_spent,
    ROUND(AVG(o.order_amount), 2) AS avg_order_amount,
    MAX(o.created_at) AS last_order_date,
    MIN(o.created_at) AS first_order_date
FROM {{ ref('stg_users') }} u
LEFT JOIN {{ ref('stg_orders') }} o
    ON u.user_id = o.user_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
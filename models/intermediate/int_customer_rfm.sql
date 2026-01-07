-- models/intermediate/int_customer_rfm.sql
-- RFM分析による顧客セグメント

WITH customer_orders AS (
    SELECT
        user_id,
        order_id,
        created_at,
        order_amount,
        status
    FROM {{ ref('stg_orders') }}
    WHERE status = 'complete'
),

rfm_metrics AS (
    SELECT
        user_id,
        
        -- Recency: 最終購入日からの経過日数
        DATE_DIFF(
            CURRENT_DATE(),
            MAX(DATE(created_at)),
            DAY
        ) AS recency_days,
        
        -- Frequency: 購入回数
        COUNT(DISTINCT order_id) AS frequency,
        
        -- Monetary: 累計購入金額
        ROUND(SUM(order_amount), 2) AS monetary,
        
        -- 最初の購入日
        MIN(DATE(created_at)) AS first_order_date,
        
        -- 最後の購入日
        MAX(DATE(created_at)) AS last_order_date
        
    FROM customer_orders
    GROUP BY user_id
),

rfm_scores AS (
    SELECT
        *,
        
        -- RFMスコア（5段階評価）
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency) AS f_score,
        NTILE(5) OVER (ORDER BY monetary) AS m_score,
        
        -- 顧客タイプ
        CASE
            WHEN COUNT(DISTINCT user_id) OVER (PARTITION BY user_id) = 1 
                AND frequency = 1 THEN 'New'
            WHEN frequency >= 3 THEN 'Loyal'
            ELSE 'Existing'
        END AS customer_type
        
    FROM rfm_metrics
),

final AS (
    SELECT
        user_id,
        recency_days,
        frequency,
        monetary,
        r_score,
        f_score,
        m_score,
        
        -- RFM総合スコア
        CONCAT(
            CAST(r_score AS STRING),
            CAST(f_score AS STRING),
            CAST(m_score AS STRING)
        ) AS rfm_score,
        
        customer_type,
        first_order_date,
        last_order_date
        
    FROM rfm_scores
)

SELECT * FROM final
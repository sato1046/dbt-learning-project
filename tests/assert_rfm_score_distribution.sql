{{ config(severity='warn') }}

-- tests/assert_rfm_score_distribution.sql
-- 各RFMスコア（1-5）に少なくとも顧客が存在することを検証

WITH score_distribution AS (
    SELECT
        r_score,
        f_score,
        m_score,
        COUNT(*) AS customer_count
    FROM {{ ref('int_customer_rfm') }}
    GROUP BY r_score, f_score, m_score
),

expected_scores AS (
    SELECT 1 AS score UNION ALL
    SELECT 2 UNION ALL
    SELECT 3 UNION ALL
    SELECT 4 UNION ALL
    SELECT 5
)

SELECT
    e.score,
    'r_score' AS score_type
FROM expected_scores e
LEFT JOIN (SELECT DISTINCT r_score FROM score_distribution) r ON e.score = r.r_score
WHERE r.r_score IS NULL

UNION ALL

SELECT
    e.score,
    'f_score' AS score_type
FROM expected_scores e
LEFT JOIN (SELECT DISTINCT f_score FROM score_distribution) f ON e.score = f.f_score
WHERE f.f_score IS NULL

UNION ALL

SELECT
    e.score,
    'm_score' AS score_type
FROM expected_scores e
LEFT JOIN (SELECT DISTINCT m_score FROM score_distribution) m ON e.score = m.m_score
WHERE m.m_score IS NULL
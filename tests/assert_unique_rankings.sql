-- tests/assert_unique_rankings.sql
-- カテゴリ内ランキングに重複がないことを検証

WITH ranking_counts AS (
    SELECT
        category,
        revenue_rank_in_category,
        COUNT(*) AS rank_count
    FROM {{ ref('int_product_performance') }}
    GROUP BY category, revenue_rank_in_category
    HAVING COUNT(*) > 1
)

SELECT * FROM ranking_counts
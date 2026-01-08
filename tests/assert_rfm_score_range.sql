-- tests/assert_rfm_score_range.sql
-- RFMスコアが1-5の範囲内であることを検証

SELECT
    user_id,
    r_score,
    f_score,
    m_score
FROM {{ ref('int_customer_rfm') }}
WHERE r_score NOT BETWEEN 1 AND 5
   OR f_score NOT BETWEEN 1 AND 5
   OR m_score NOT BETWEEN 1 AND 5
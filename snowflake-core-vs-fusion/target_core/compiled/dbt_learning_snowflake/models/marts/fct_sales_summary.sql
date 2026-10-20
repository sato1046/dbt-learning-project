-- チャネル × 顧客セグメント × 商品カテゴリ 別の売上サマリ
with sales as (

    select * from dbt_learning.dbt_learning.stg_sales

)

select
    channel,
    customer_segment,
    product_category,
    count(*)            as transaction_count,
    sum(amount)         as total_amount,
    avg(amount)         as avg_amount,
    min(amount)         as min_amount,
    max(amount)         as max_amount
from sales
group by
    channel,
    customer_segment,
    product_category
order by total_amount desc
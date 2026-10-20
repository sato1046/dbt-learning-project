-- 月次の売上集計
with sales as (

    select * from dbt_learning.dbt_learning.stg_sales

)

select
    order_month,
    count(*)    as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
from sales
where order_month is not null
group by order_month
order by order_month

  create or replace   view dbt_learning.dbt_learning.stg_sales
  
  
  
  
  as (
    with source as (

    select * from dbt_learning.dbt_learning.raw_sales

),

renamed as (

    select
        transaction_id,
        try_to_date(order_date) as order_date,
        date_trunc('month', try_to_date(order_date)) as order_month,
        channel,
        customer_segment,
        product_category,
        amount

    from source

)

select * from renamed
  );


with source as (

    select * from {{ source('raw', 'raw_sales') }}

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

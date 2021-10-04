with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select 
        orderid,
        sum(CASE when status = 'success' then amount end) amount
    from {{ ref('stg_payments') }}
    group by orderid
),

final as (
    select 
        orders.order_id,
        orders.customer_id,
        coalesce(payments.amount, 0) as total_amount
    from orders
    left join payments on orders.order_id = payments.orderid
)

select * from final
    
{{config(
    materialized="table"
)}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),


customer_orders as (

    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

customer_payments as (

    select
        orders.customer_id,
        sum(CASE when payments.status = 'success' then amount end) lifetime_value
    from orders
	inner join payments ON orders.order_id = payments.orderid
    group by orders.customer_id

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
		coalesce(customer_payments.lifetime_value/100, 0) as lifetime_value

    from customers

    left outer join customer_orders ON customers.customer_id = customer_orders.customer_id
	left outer join customer_payments ON customers.customer_id = customer_payments.customer_id

)

select * from final
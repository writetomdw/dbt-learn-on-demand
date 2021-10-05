with payments as (
    select 
        id as payment_id,
        orderid,
        paymentmethod,
        status,
        {{ cents_to_dollars('amount') }} as amount
    from raw.stripe.payment
)

select * from payments


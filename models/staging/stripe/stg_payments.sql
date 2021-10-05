with payments as (
    select 
        id as payment_id,
        orderid,
        paymentmethod,
        status,
        amount
    from raw.stripe.payment
)

select * from payments


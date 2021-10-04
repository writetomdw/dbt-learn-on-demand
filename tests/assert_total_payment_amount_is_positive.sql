select 
    orderid,
    sum(amount) as total_amount
    from {{ ref('stg_payments') }}
    group by 1
    having NOT total_amount >= 0 
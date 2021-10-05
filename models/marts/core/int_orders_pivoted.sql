{%- set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] -%}
        with payments as (
    select * from {{ ref('stg_payments') }}
),

pivoted as ( 
    select
        orderid,
        {% for payment_method in payment_methods %}
        {%- if loop.last -%}
            sum(case when paymentmethod = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount
        {%- else -%}
            sum(case when paymentmethod = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endif %}
        {%- endfor %}

      /*  sum(case when paymentmethod = 'bank_transfer' then amount else 0 end) as bank_transfer_amount */

    from payments
    where status = 'success'
    group by orderid
)

select * from pivoted
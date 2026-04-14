with

orders as (
    select *
    from {{ ref('orders') }}
),

null_orders_status as (
    select 
        status,
        count(*) as total_orders,
        countif(delivered_carrier_date is null) as null_carrier_date,
        countif(customer_delivery_date is null) as null_customer_date,
        countif(approved_at is null) as null_approved
    from orders
    group by 1
    order by 2 desc
),

unexpected_values_orders as (
    select *
    from orders
    where (approved_at < purchase_timestamp)
    or (delivered_carrier_date < approved_at)
    or (customer_delivery_date < delivered_carrier_date)
    or (customer_delivery_date < purchase_timestamp)
),

unexpected_order_counts as (
    select 
        countif(approved_at < purchase_timestamp) as approved_before_purchase,
        countif(delivered_carrier_date < approved_at) as shipped_before_approved,
        countif(customer_delivery_date < delivered_carrier_date) as delivered_before_shipped,
        countif(customer_delivery_date < purchase_timestamp) as delivered_before_purchase
    from unexpected_values_orders
),

payments as (
    select *
    from {{ ref('payments') }}
),

undefined_payments as (
    -- only three exist, they are all orders that were canceled
    select * 
    from payments p
    join orders o 
        on o.order_id = p.order_id
    where payment_type = 'not_defined'
), 

payment_stats as (
    -- no negative payments, most payments are by credit 
    select 
        count(*) as total_payments,
        sum(payment_value) as sum_amount,
        avg(payment_value) as avg_amount,
        min(payment_value) as min_payment,
        max(payment_value) as max_payment,
        sum(case when payment_type != 'credit_card' then payment_value end) as non_credit,
        sum(case when payment_type = 'credit_card' then payment_value end) as credit
    from payments 
),

payment_nulls as (
    -- no null values returned 
    select 
        countif(order_id is null) as null_order_id,
        countif(payment_sequential is null) as null_payment_seq,
        countif(payment_type is null) as null_type,
        countif(payment_installments is null) as null_installments,
        countif(payment_value is null) as null_value
    from payments
),

items as (
    select *
    from {{ ref('items') }}
)

select * from items


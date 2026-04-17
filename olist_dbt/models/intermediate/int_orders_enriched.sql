/* 
int_orders_enriched
--------------------------------
order_id
customer_id
order_status
ordered_at

-- payment signals
total_paid
payment_method
payment_count

-- review signals
review_score
is_negative_review

*/

with orders as(
    select 
        order_id,
        customer_id,
        status,
        purchase_timestamp,
        approved_at,
        delivered_carrier_date,
        customer_delivery_date,
        estimated_delivery
    from {{ref('stg_orders')}}
),

reviews as(
    select 
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        reviewed_at
    from {{ref('int_order_reviews')}}
),

customers as (
    select 
        customer_id,
        customer_unique_id
    from {{ref('stg_customers')}}
),
payments as(
    select 
        order_id,
        payment_count,
        total_paid,
        primary_payment_type,   
        payment_method
    from {{ref('int_order_payments')}}
)

select 
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    o.status as order_status,
    o.purchase_timestamp as ordered_at,

    --payments 
    p.payment_count,
    p.total_paid,
    p.primary_payment_type,
    p.payment_method,

    -- reviews
    r.review_score,
    case
        when review_score is null then null
        when review_score <= 3 then 1
        else 0
    end as is_negative_review,

    --timestamps 
    o.purchase_timestamp,
    o.approved_at,
    o.delivered_carrier_date,
    o.customer_delivery_date,
    o.estimated_delivery,

    --deliveries
    timestamp_diff(o.customer_delivery_date, o.purchase_timestamp, DAY) as days_to_delivery,
    timestamp_diff(o.estimated_delivery, o.purchase_timestamp, DAY) as estimated_delivery_days,
    timestamp_diff(o.delivered_carrier_date, o.purchase_timestamp, DAY) as days_to_carrier,
    timestamp_diff(o.customer_delivery_date, o.delivered_carrier_date, DAY) as days_carrier_to_customer,
    timestamp_diff(approved_at, purchase_timestamp, DAY) as days_to_approval, 
    timestamp_diff(customer_delivery_date, estimated_delivery, DAY) as days_late  -- negative if early, positive if late
from orders o
left join payments p on o.order_id = p.order_id
left join reviews r on r.order_id = o.order_id
left join customers c on c.customer_id = o.customer_id


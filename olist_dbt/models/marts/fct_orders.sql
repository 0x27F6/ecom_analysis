/*
Description: order level fact table
grain: one row per order
primary key: order_id

-- keys
order_id
customer_id
customer_unique_id

-- order info
order_status
ordered_at

-- payment signals
payment_count
total_paid
primary_payment_type
payment_method

-- review signals
review_score
is_negative_review

-- delivery metrics
days_to_approval
days_to_carrier
days_carrier_to_customer
days_to_delivery
days_late
*/

select
    -- keys
    order_id,
    customer_id,
    customer_unique_id,

    -- order info
    order_status,
    ordered_at,

    -- payment signals
    payment_count,
    total_paid,
    primary_payment_type,
    payment_method,

    -- review signals
    review_score,
    is_negative_review,

    --timestamps 
    purchase_timestamp,
    approved_at,
    delivered_carrier_date,
    customer_delivery_date,
    estimated_delivery,

    -- delivery metrics
    days_to_approval,
    days_to_carrier,
    days_carrier_to_customer,
    days_to_delivery,
    days_late

from {{ref('int_orders_enriched')}}

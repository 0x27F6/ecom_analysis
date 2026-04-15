with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

payments as (
    -- if you want AOV or payment method mix later
    select * from {{ ref('stg_payments') }}
)

select
    -- ids
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    o.customer_id,
    
    -- dates
    o.ordered_at,
    o.delivered_at,
    date_diff('day', o.purchase_timestamp, o.customer_delivery_date) as days_to_deliver,
    date_diff('day', o.purchase_timestamp, o.estimated_delivery) as promised_delivery_days,
    date_diff('day', o.purchase_timestamp, o.delivered_carrier_date) as time_to_carrier, -- can be used to help determine where supply chain bottle necks occur
    date_diff('day', o.delivered_carrier_date, o.customer_delivery_date) as time_carrier_to_customer,

    -- money
    oi.item_price,
    oi.freight_cost,
    oi.item_price + oi.freight_cost as gross_revenue,
    
    -- satisfaction
    r.review_score,
    r.review_comment_message,
    
    -- flags
    o.order_status,
    case when o.delivered_at > o.estimated_delivery_at then 1 else 0 end as is_late_delivery,
    case when r.review_score <= 3 then 1 else 0 end as is_negative_review

from order_items oi
left join orders o on oi.order_id = o.order_id
left join reviews r on oi.order_id = r.order_id 


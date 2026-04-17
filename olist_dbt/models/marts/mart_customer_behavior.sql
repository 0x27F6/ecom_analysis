/*
description: measures customer behavior and order stats
grain: one row per customer
primary key: customer_unique_id

metrics:
-- volume
total_orders
total_items_ordered

-- spend
lifetime_spent (total_paid from orders - buyer perspective)
lifetime_purchased (sum of item prices - seller perspective)
lifetime_freight
spend_vs_purchased_delta (difference between buyer and seller perspective)
avg_payment_installments

-- reviews
review_count
avg_review_score
negative_review_count

-- delivery
avg_days_late (across all orders, negative = early)
num_late_orders

-- tenure
first_order_date
last_order_date
repeat_customer

*/

with 

orders as (
    select 
        order_id,
        customer_unique_id,
        order_status,
        total_paid,
        payment_count,
        review_score,
        is_negative_review,
        days_late
    from {{ref('fct_orders')}}
),

items as (
    select 
        order_id,
        count(*) as total_order_items,
        sum(price) as price_seller_perspective,
        sum(freight_value) as order_total_freight
    from {{ref('fct_order_items')}}
    group by 1
),

order_metrics as (
    select 
        o.order_id,
        o.customer_unique_id,
        o.total_paid,
        o.payment_count,
        o.review_score,
        o.is_negative_review,
        o.days_late,
        i.total_order_items,
        i.price_seller_perspective,
        i.order_total_freight
    from orders o 
    left join items i on o.order_id = i.order_id
    -- cancelled orders excluded to avoid skewing aggregations
    where o.order_status != 'canceled'
),

customer_metrics as (
    select 
        customer_unique_id,
        -- volume
        count(distinct order_id) as total_orders,
        sum(total_order_items) as total_items_ordered,

        -- spend
        round(sum(total_paid), 2) as lifetime_spent,
        round(sum(price_seller_perspective), 2) as lifetime_purchased,
        round(sum(order_total_freight), 2) as lifetime_freight,
        round(sum(total_paid) - sum(price_seller_perspective), 2) as spend_vs_purchased_delta,
        round(avg(payment_count), 2) as avg_payment_installments,

        -- reviews
        count(review_score) as review_count,
        round(avg(review_score), 2) as avg_review_score,
        sum(is_negative_review) as negative_review_count,

        -- delivery
        round(avg(days_late), 2) as avg_days_late,
        sum(case when days_late > 0 then 1 else 0 end) as num_late_orders

    from order_metrics
    group by 1  
)

select 
    cm.customer_unique_id,

    -- volume
    cm.total_orders,
    cm.total_items_ordered,

    -- spend
    cm.lifetime_spent,
    cm.lifetime_purchased,
    cm.lifetime_freight,
    cm.spend_vs_purchased_delta, -- should be 1:1 with lifetime_freight 
    cm.avg_payment_installments,

    -- reviews
    cm.review_count,
    cm.avg_review_score,
    cm.negative_review_count,

    -- delivery
    cm.avg_days_late,
    cm.num_late_orders,

    -- tenure
    dc.first_order_date,
    dc.last_order_date,
    case when cm.total_orders > 1 then 1 else 0 end as repeat_customer

from customer_metrics cm
left join {{ref('dim_customers')}} dc on dc.customer_unique_id = cm.customer_unique_id

    
   

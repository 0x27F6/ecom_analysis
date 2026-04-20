/*
description: product level performance metrics
grain: one row per product
primary key: product_id

note: review scores are at the order level and may reflect other products 
in the same order. single_item_orders indicates the count of orders where 
the review unambiguously reflects this product only.

metrics:
-- volume
units_sold
lifetime_orders
single_item_orders

-- revenue
lifetime_revenue
lifetime_freight
freight_to_revenue_ratio

-- delivery
avg_days_to_carrier
avg_days_to_delivery
avg_days_late
avg_days_vs_sla
sla_breaches
sla_breach_rate

-- reviews (order level, see note)
review_count
avg_review_score
negative_review_count
*/

with 

products as (
    select *
    from {{ref('fct_order_items')}}
),

order_item_counts as (
    select
        order_id,
        count(*) as items_in_order
    from {{ref('fct_order_items')}}
    group by order_id
),

orders as (
    select 
        order_id,
        review_score,
        is_negative_review
    from {{ref('fct_orders')}}
    where order_status != 'canceled'
),

product_metrics as (
    select 
        p.product_id,
        p.product_category,
        -- volume
        count(*) as units_sold,
        count(distinct p.order_id) as lifetime_orders,
        sum(case when oc.items_in_order = 1 then 1 else 0 end) as single_item_orders,
        -- revenue
        round(sum(p.price), 2) as lifetime_revenue,
        round(sum(p.freight_value), 2) as lifetime_freight,
        round(sum(p.freight_value) / nullif(sum(p.price), 0), 4) as freight_to_revenue_ratio,
        -- delivery
        round(avg(p.days_to_carrier), 2) as avg_days_to_carrier,
        round(avg(p.days_to_delivery), 2) as avg_days_to_delivery,
        round(avg(p.days_late), 2) as avg_days_late,
        round(avg(p.days_to_carrier_vs_sla), 2) as avg_days_vs_sla,
        sum(case when p.days_to_carrier_vs_sla > 0 then 1 else 0 end) as sla_breaches,
        round(sum(case when p.days_to_carrier_vs_sla > 0 then 1 else 0 end) / count(*), 4) as sla_breach_rate,
        -- reviews
        count(o.review_score) as review_count,
        round(avg(o.review_score), 2) as avg_review_score,
        sum(o.is_negative_review) as negative_review_count
    from products p
    left join order_item_counts oc on oc.order_id = p.order_id
    left join orders o on o.order_id = p.order_id
    group by 1, 2
)

select *
from product_metrics
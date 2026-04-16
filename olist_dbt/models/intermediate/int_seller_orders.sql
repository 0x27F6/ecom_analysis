/*
Description: seller level data 
grain: one row per seller

seller_id
seller_zip
seller_city
seller_state

total_items_sold
unique_order_count
total_revenue
total_freight
review_count
avg_review_score
avg_days_to_carrier

*/

with sellers as(
    select 
        seller_id,
        seller_zip,
        seller_city,
        seller_state
    from {{ref('stg_sellers')}}
),

-- reviews happen at the order level. multiple sellers inherit the same review score for a shared order
seller_stats as (
    select
        oi.seller_id,
        count(*) as total_items_sold,
        count(distinct oi.order_id) as unique_order_count,
        round(sum(oi.price), 2) as total_revenue,
        round(sum(oi.freight_value), 2) as total_freight,
        round(avg(oe.review_score), 1) as avg_review_score,
        count(oe.review_score) as review_count,
        round(avg(oe.days_to_carrier), 1) as avg_days_to_carrier
    from {{ref('int_order_items_enriched')}} oi
    left join {{ref('int_orders_enriched')}} oe on oe.order_id = oi.order_id
    group by oi.seller_id
)

select 
    s.seller_id,
    s.seller_zip,
    s.seller_city,
    s.seller_state,
    ss.total_items_sold,
    ss.unique_order_count,
    ss.total_revenue,
    ss.total_freight,
    ss.avg_review_score,
    ss.review_count,
    ss.avg_days_to_carrier
from sellers s
left join seller_stats ss on s.seller_id = ss.seller_id


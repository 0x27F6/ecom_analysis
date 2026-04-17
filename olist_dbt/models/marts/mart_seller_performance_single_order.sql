/*
Description: Tracks the performance of single seller order
Grain: one seller per row 

Note: reviews are at the order level. in order to honestly account for seller performance bundled orders 
are filtered. Performance can then be isolated to determine if there are violations of SLA or product quality is 
is poor.

metrics:
-- volume
total_orders
orders_fulfilled
fulfillment_rate

-- SLA
sla_breaches          -- orders where days_vs_shipping_sla > 0
sla_compliance_rate   -- 1 - (sla_breaches / total_orders)
avg_days_vs_sla       -- how late on average, negative is early

-- quality
review_count
avg_review_score

-- revenue
total_sold
total_freight
avg_order_value 

filters:
delivered orders
single seller 

*/

with 

single_seller_orders as(
    select order_id
    from {{ref('fct_order_items')}}
    group by order_id
    having count(distinct seller_id) = 1 

),

orders as (
    select 
        order_id,
        ordered_at,
        review_score, 
        is_negative_review,
        order_status,
        days_to_carrier,
        days_carrier_to_customer, 
        days_to_delivery,
        days_late
    from {{ref('fct_orders')}} 
    where order_id in (select order_id from single_seller_orders)
),

items as (
    select 
        order_id,
        seller_id,
        count(order_item_id) as items_in_orders,
        sum(price) total_price,
        sum(freight_value) total_freight,
        min(days_to_carrier_vs_sla) as days_to_carrier_vs_sla
    from {{ref('fct_order_items')}}
    where order_id in (select order_id from single_seller_orders)
    group by order_id, seller_id
),

order_seller_metrics as(
select 
    o.order_id,
    i.seller_id,

    --order details
    i.total_price,
    i.total_freight,
    i.items_in_orders,

    --shipping
    o.ordered_at,
    o.order_status,
    o.days_to_carrier,
    i.days_to_carrier_vs_sla,
    
    --reviews 
    o.review_score,
    o.is_negative_review
from orders o
join items i on o.order_id = i.order_id
)

select 
    seller_id,
    -- volume
    count(*) as total_orders,
    count(case when days_to_carrier is not null then 1 end) as fulfillment_count,
    round(count(case when days_to_carrier is not null then 1 end) / count(*), 2) as fullfillment_rate,

    --SLA 
    sum(case when days_to_carrier_vs_sla > 0 then 1 else 0 end) as sla_breaches,
    round(1 - (sum(case when days_to_carrier_vs_sla > 0 then 1 else 0 end) / count(*)), 2)as sla_compliance_rate,
    round(avg(days_to_carrier_vs_sla), 2) as avg_days_vs_sla, -- positive late | negative early 

    -- revenue 
    round(sum(total_price), 2) as lifetime_revenue,
    -- freight values passed onto customers
    round(sum(total_freight), 2) as lifetime_freight,
    round(avg(total_price), 2) as avg_order_value,

    -- reviews
    count(review_score) as total_reviews,
    round(avg(review_score), 2) as avg_reviews,
    sum(is_negative_review) as total_negative_reviews,
    round(sum(is_negative_review) / count(review_score), 2) as negative_review_rate
from order_seller_metrics 
group by seller_id
    


/*
grain: one row per customer
description: customer stats
primary_key: customer_unique_id 
---------------
--keys 
customer_unique_id

-- stats
total order
avg review
total reviews 
first order 
last order 

-- location
customer_zip_code
customer_city
customer_state

*/

with    
customer as(
    select distinct -- data is static so distinct is a fine approach
        customer_unique_id,
        customer_zip_code,
        customer_city,
        customer_state
    from {{ref('stg_customers')}}
),

orders as (
    select 
        customer_unique_id,
        count(*) as total_orders,
        -- these review metrics DO NOT account for updated reviews
        count(review_score) as total_reviews,
        round(avg(review_score),1) as avg_review,
        min(ordered_at) as first_order_date,
        max(ordered_at) last_order_date
    from {{ref('int_orders_enriched')}}
    group by customer_unique_id
)

select 
    c.customer_unique_id,
    -- stats
    o.total_orders,
    o.total_reviews,
    o.avg_review,
    o.first_order_date,
    o.last_order_date,

    --location
    c.customer_zip_code,
    c.customer_city,
    c.customer_state
from customer c
left join orders o on o.customer_unique_id = c.customer_unique_id
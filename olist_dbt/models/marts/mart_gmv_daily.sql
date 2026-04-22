/*
description: daily business activity metrics for exec kpi strip
grain: one row per day
primary key: order_date

note: coincident activity metrics only — all values complete as of end of day.
review metrics excluded due to different time semantic (reviews arrive days/weeks
after order and would make recent days artificially incomplete).

metrics:
gmv                    -- sum of item prices, cancelled orders excluded
order_count            -- distinct orders placed that day
active_customers       -- distinct customers who ordered that day
returning_customers    -- active customers with at least one prior order
*/

with

orders as (
    select
        order_id,
        customer_unique_id,
        cast(ordered_at as date) as order_date,
        row_number() over (
            partition by customer_unique_id
            order by ordered_at
        ) as order_seq
    from {{ ref('fct_orders') }}
    where order_status != 'canceled'
),

items as (
    select
        order_id,
        sum(price) as order_gmv
    from {{ ref('fct_order_items') }}
    group by 1
),

orders_enriched as (
    select
        o.order_date,
        o.order_id,
        o.customer_unique_id,
        o.order_seq,
        i.order_gmv
    from orders o
    left join items i on o.order_id = i.order_id
),

daily as (
    select
        order_date,
        round(sum(order_gmv), 2) as gmv,
        count(distinct order_id) as order_count,
        count(distinct customer_unique_id) as active_customers,
        count(distinct case when order_seq > 1 then customer_unique_id end) as returning_customers
    from orders_enriched
    group by 1
)

select *
from daily
order by order_date
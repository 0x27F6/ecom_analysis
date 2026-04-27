/*
description: seller revenue concentration and power law distribution
grain: one row per seller
primary key: seller_id

note: includes all orders regardless of single/multi-seller to accurately 
represent total seller revenue on the platform.

metrics:
revenue_rank
lifetime_revenue
cumulative_revenue_share
total_orders
*/

with seller_revenue as (
    select
        seller_id,
        count(distinct order_id) as total_orders,
        round(sum(price), 2) as lifetime_revenue
    from {{ ref('fct_order_items') }}
    group by seller_id
)

select
    seller_id,
    total_orders,
    lifetime_revenue,
    row_number() over (order by lifetime_revenue desc) as revenue_rank,
    round(
        sum(lifetime_revenue) over (order by lifetime_revenue desc rows between unbounded preceding and current row)
        / sum(lifetime_revenue) over ()
        * 100, 2
    ) as cumulative_revenue_share
from seller_revenue
order by revenue_rank
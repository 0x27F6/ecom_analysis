select
    cast(o.ordered_at as date) as order_date,
    sum(i.price) as gmv,
    count(distinct o.order_id) as order_count
from {{ ref('fct_orders') }} o
join {{ ref('fct_order_items') }} i
    using (order_id)
where o.order_status != 'canceled'
group by 1
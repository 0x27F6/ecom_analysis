/*
description: product affinity analysis - products most commonly purchased together
grain: one row per product pair
primary key: (product_a_id, product_b_id)

note: product_a_id is always less than product_b_id to prevent duplicate pairs.
minimum co-occurrence threshold of 10 applied to filter noise.

metrics:
times_bought_together
product_a_category
product_b_category
same_category_flag
*/

with 

order_items as (
    select
        order_id,
        product_id,
        product_category
    from {{ref('fct_order_items')}}
),

product_pairs as (
    select
        a.product_id as product_a_id,
        a.product_category as product_a_category,
        b.product_id as product_b_id,
        b.product_category as product_b_category,
        count(*) as times_bought_together
    from order_items a
    join order_items b
        on a.order_id = b.order_id
        and a.product_id < b.product_id
    group by 1, 2, 3, 4
    having count(*) > 3
)

select
    product_a_id,
    product_a_category,
    product_b_id,
    product_b_category,
    times_bought_together,
    case when product_a_category = product_b_category then 1 else 0 end as same_category_flag
from product_pairs
order by times_bought_together desc
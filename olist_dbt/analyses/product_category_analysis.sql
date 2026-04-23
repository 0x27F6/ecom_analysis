/*
description: category analysis to determine if category is resulting in poor review scores
grain: one row per product category

review score is at order level; category attribution assumes the review reflects the dominant or 
sole product category in that order. Multi-category orders are included and attributed to the first/primary 
item.

findings: Evry single order across categories is early so negative review scores aren't related to delivery at all. If
anything is seems like olist is systematically sand bagging delivery dates so oders come in earlier than expect, though
this may not be the full picture.

1/5 orders results in a negative review -- quite a high rate

Data that would help triangulate what leads to such a high negative review rate:
- review data at the item grain instead of order grain, this helps pinpoint problem sellers and items
- return/refund data - did sellers who respond to negative reviews have better subsequent scores

*/

with order_category as (
    select
        oi.product_id,
        oi.product_category,
        o.order_id,
        o.review_score,
        o.is_negative_review,
        o.days_late
    from {{ ref('fct_orders') }} o
    inner join {{ ref('fct_order_items') }} oi 
        on o.order_id = oi.order_id 
        and oi.order_item_id = 1
)


select
    product_category,
    count(*) as total_orders,
    round(avg(review_score), 2) as avg_review_score,
    sum(is_negative_review) as total_negative_reviews,
    round(sum(is_negative_review) * 100.0 / count(*), 2) as negative_review_rate,
    round(avg(days_late), 2) as avg_days_late,
    round(approx_percentile(sum(is_negative_review) * 100.0 / count(*)) over(), 2) as  olist_median_negative_review_rate
from order_category
group by 1
order by total_orders desc


/*
calculate median review rate platorm wide 

category_rates as (
    select
        product_category,
        round(sum(is_negative_review) * 100.0 / count(*), 2) as negative_review_rate
    from order_category
    group by 1
)

select
    approx_quantiles(negative_review_rate, 100)[offset(50)] as median_negative_review_rate
from category_rates
*/

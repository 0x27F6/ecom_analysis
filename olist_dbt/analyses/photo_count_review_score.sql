/*
description: photo count vs review score analysis
grain: one row per product_photos_qty value

hypothesis: products with fewer photos lead to higher negative review rates
due to customer expectation mismatch

findings: hypothesis not supported. the difference between 1 photo (23% negative rate) 
and 5 photos (19.4% negative rate) is 3.5 percentage points — directionally consistent 
but not a meaningful effect. sample sizes above 10 photos are too small to be conclusive.
photo count is not a significant driver of negative reviews.

this exhausts structural drivers available in this dataset. negative review rate of ~21% 
is consistent across delivery performance, seller SLA compliance, product category, and 
photo count. proper attribution requires item-level review scores or text analysis of 
portuguese review comments — both outside scope of this analysis.
*/


with order_category as (
    select
        oi.product_photos_qty,
        o.review_score,
        o.is_negative_review
    from {{ ref('fct_orders') }} o
    inner join {{ ref('fct_order_items') }} oi 
        on o.order_id = oi.order_id 
        and oi.order_item_id = 1
)

select
    product_photos_qty,
    count(*) as total_orders,
    round(avg(review_score), 2) as avg_review_score,
    round(sum(is_negative_review) * 100.0 / count(*), 2) as negative_review_rate
from order_category
group by 1
order by 1
/*
description: sla compliance distribution buckets 
grain: one row per compliance bucket 

hypothesis prior to query: sellers below the SLA compliance rate were causing low review score. DISPROVEN
SLA compliance rate is a weak predictor of negative review impact. The below_75% cohort has the worst per-order
review rate (0.42) but generates only 4% of negative reviews due to low volume. High-volume sellers in the 90-99% 
compliance range generate 53% of all negative reviews. Platform review score is a volume problem, 
not purely a compliance problem.

potential slices: product quality or by category, 
*/
select
    case
        when sla_compliance_rate = 1.0 then 'perfect'
        when sla_compliance_rate >= 0.9 then '90-99%'
        when sla_compliance_rate >= 0.75 then '75-89%'
        when sla_compliance_rate < 0.75 then 'below_75%'
    end as compliance_bucket,
    count(*) as seller_count,
    round(count(*) * 100.0 / sum(count(*)) over(), 2) as pct_of_sellers,
    sum(total_orders) as bucket_total_orders,
    sum(total_negative_reviews) as bucket_negative_reviews,
    -- use a window function to sum the total of each buckets orders
    round(sum(total_negative_reviews) * 100.0 / sum(sum(total_negative_reviews)) over(), 2) as pct_of_all_negative_reviews,
    round(avg(avg_reviews), 2) as avg_review_score,
    round(avg(negative_review_rate), 2) as avg_negative_review_rate
from {{ref('mart_seller_performance_single_order')}}
group by 1
order by 1
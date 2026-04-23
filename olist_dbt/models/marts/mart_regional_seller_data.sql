/*
description: state level view of sellers count, compliance, and review
grain: one state per row

findings: min compliance is .75 with only one seller in that region. regions with a compliance between 80-90% 
have slightly higher regional review scores. regional SLA variance does not appear to drive regional review score differences.
the problem is not geographic infrastructure.next investigation will be into sellers or specific products / categories to pinpoint 
where poor reviews come from. 



*/

with

seller_performace as(
    select 
        seller_id,
        sla_compliance_rate,
        avg_reviews
    from {{ref('mart_seller_performance_single_order')}}
),

seller_state as(
    select 
        seller_id,
        seller_state -- more granular than states 
    from {{ref('dim_sellers')}}
)

select 
    sc.seller_state as state,
    count(sp.seller_id) as total_sellers, 
    round(avg(sla_compliance_rate), 2) as regional__sla_compliance,
    round(avg(avg_reviews), 2) as regional_review_score ,
from seller_performace sp
left join seller_state sc
    on sc.seller_id = sp.seller_id
group by 1
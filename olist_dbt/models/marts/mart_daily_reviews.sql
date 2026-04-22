/*
description: daily review activity for exec kpi card
grain: one row per day (by review_creation_date)
primary key: review_date

note: attributed by review_creation_date, not order date. measures
incoming sentiment as it arrives rather than sentiment for a given
order cohort. reviews arrive days to weeks after order placement,
so this metric lags business activity.

source grain: dim_reviews is one row per order; a single review_id
may span multiple orders in the Olist dataset (merchant bundling),
so each order contributes its review_score once regardless of 
shared review_id.

filters: null review scores excluded — not every order is reviewed
(~88% review rate in Olist). excluded rows do not contribute to
either the numerator or denominator.

metrics:
avg_review_score    -- simple average of scores received that day
review_count        -- number of reviews received that day (denom
                       for weighted rollup in bi layer)
negative_reviews    -- reviews with score <= 2 (for detail views
                       and proportion-of-negative tracking)
*/

with

reviews as (
    select
        cast(review_creation_date as date) as review_date,
        review_score
    from {{ ref('dim_reviews') }}
    where review_score is not null
),

daily as (
    select
        review_date,
        round(avg(review_score), 3) as avg_review_score,
        count(*) as review_count,
        sum(case when review_score <= 2 then 1 else 0 end) as negative_reviews
    from reviews
    group by 1
)

select *
from daily
order by review_date
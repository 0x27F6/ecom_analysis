/*
The stg_order_reviews table contains multiple inconsistencies that prevent it from being safely 
joined at the order grain without transformation.

Two duplication patterns are observed:
	•	Some review_id values appear across multiple order_ids
	•	Some order_ids have multiple associated review records

This indicates that the raw dataset does not enforce a strict one-to-one relationship between reviews and orders.

To enable reliable downstream joins, this model is conformed to the order grain (order_id).

Where multiple reviews exist for a single order, the most recent review (by review_answer_timestamp) is selected. 
This approach assumes the latest review best reflects the customer’s final assessment of their experience.

*/

with reviews as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    from {{ ref('stg_order_reviews') }}
),

ranked_reviews as (
    select
        *,
        row_number() over (
            partition by order_id
            order by review_answer_timestamp desc, review_id desc
        ) as rn
    from reviews
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp as reviewed_at
from ranked_reviews
where rn = 1



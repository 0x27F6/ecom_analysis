/*
Description: review dimension
grain: one row per order
primary key: review_id
*/

select
    -- keys
    review_id,
    order_id,

    -- review content
    review_score,
    review_comment_title,
    review_comment_message,

    -- timestamps
    review_creation_date,
    reviewed_at

from {{ref('int_order_reviews')}}
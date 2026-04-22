/*
description: review dimension — latest review per order
grain: one row per order
primary key: order_id
note: review_id is NOT unique in this dimension — the Olist dataset allows 
a single review to span multiple related orders. order_id is the true PK.
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
/*
Description: seller dimension
grain: one row per seller
primary key: seller_id
*/

select
    -- keys
    seller_id,

    -- stats
    total_items_sold,
    unique_order_count,
    total_revenue,
    total_freight,
    review_count,
    avg_review_score,
    avg_days_to_carrier,

    -- location
    seller_zip,
    seller_city,
    seller_state

from {{ref('int_seller_orders')}}
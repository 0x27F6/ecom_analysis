/*
Description: customer dimension
grain: one row per customer
primary key: customer_unique_id
*/

select
    -- keys
    customer_unique_id,

    -- stats
    total_orders,
    total_reviews,
    avg_review,
    first_order_date,
    last_order_date,

    -- location
    customer_zip_code,
    customer_city,
    customer_state

from {{ref('int_customer_orders')}}
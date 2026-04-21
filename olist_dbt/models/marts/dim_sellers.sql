/*
Description: seller dimension
grain: one row per seller
primary key: seller_id
*/

select
    -- keys
    seller_id,

    -- location
    seller_zip,
    seller_city,
    seller_state

from {{ref('int_seller_orders')}}
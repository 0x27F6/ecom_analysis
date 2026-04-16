/*
Description: product dimension
grain: one row per product
primary key: product_id

*/

select
    -- keys
    product_id,

    -- attributes
    product_category,
    product_name_length,
    product_description_length,
    product_photos_qty,

    -- measurements
    weight_g,
    length_cm,
    width_cm

from {{ref('int_items_enriched')}}
-- deduplicate. no meaningful tiebreaker in the data 
qualify row_number() over (partition by product_id order by product_id) = 1
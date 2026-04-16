/*
grain: one row per ordered item
keys: (order_id, order_item_id), seller_id, product_id,
------------
order_id
order_item_id
product_id
seller_id

-- item economics
price
freight_value

-- product attributes
product_category_portuguese
product_category_english

-- seller location
seller_zip
seller_city
seller_state

*/

with 

order_items as(
    select 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value,
        shipping_limit_date
    from {{ref('stg_order_items')}}
),

products as(
    select 
        product_id,
        product_category,
        product_name_length,
        product_description_length, 
        product_photos_qty,
        weight_g,
        length_cm,
        width_cm
    from {{ref('int_product_translation')}}
),

sellers as (
    select 
        seller_id,
        seller_zip,
        seller_city,
        seller_state 
    from {{ref('stg_sellers')}}
)

select 
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    -- economics
    oi.price,
    oi.freight_value,

    -- shipping
    oi.shipping_limit_date,

    -- attributes 
    p.product_category,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    --measurements
    p.weight_g,
    p.length_cm,
    p.width_cm,

    --sellers info
    s.seller_zip,
    s.seller_city,
    s.seller_state
from order_items oi
left join products p on p.product_id = oi.product_id
left join sellers s on s.seller_id = oi.seller_id







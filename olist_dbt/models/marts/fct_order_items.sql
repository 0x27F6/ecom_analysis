/*
Description: item level fact table
grain: one row per order item
primary key: (order_id, order_item_id)

note: order_status & ordered_at exist at the order grain - all items in the same order share the same status and time
of order
*/

select 
    -- keys
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    oe.customer_unique_id,

    -- order context
    oe.order_status,
    oe.ordered_at,

    -- item economics
    oi.price,
    oi.freight_value,
    oi.shipping_limit_date,

    -- product attributes
    oi.product_category,
    oi.product_name_length,
    oi.product_description_length,
    oi.product_photos_qty,
    oi.weight_g,
    oi.length_cm,
    oi.width_cm,

    -- seller location
    oi.seller_zip,
    oi.seller_city,
    oi.seller_state,

    -- delivery context
    oe.days_to_carrier,
    oe.days_to_delivery,
    oe.days_late, -- positive value: early | negative_value:late
    timestamp_diff(oe.delivered_carrier_date, oi.shipping_limit_date, DAY) as days_to_carrier_vs_sla -- positive value: early | negative_value:late

from {{ref('int_items_enriched')}} oi
left join {{ref('int_orders_enriched')}} oe on oe.order_id = oi.order_id
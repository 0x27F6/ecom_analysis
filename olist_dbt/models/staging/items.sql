with 

source as(
    select *
    from {{ref('olist_order_items_dataset')}}
)

select 
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date as date_of_shipment,
    price,
    freight_value
from source

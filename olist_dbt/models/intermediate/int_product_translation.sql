with

products as(
    select *
    from {{ref('stg_products')}}
),

translation as(
    select *
    from {{ref('stg_category_translation')}}
)

select 
    p.product_id,
    t.english_translation as product_category,
    p.product_name_length,
    p.product_description_length, 
    p.product_photos_qty,
    p.product_weight_g as weight_g,
    p.product_length_cm as length_cm,
    p.product_width_cm as width_cm
from products p
left join translation t
    on t.product_category_name = p.product_category_name


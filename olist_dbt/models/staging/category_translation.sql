with 

source as (
    select *
    from {{ref('product_category_name_translation')}}
)

select 
    product_category_name,
    product_category_name_english as english_translation
from source

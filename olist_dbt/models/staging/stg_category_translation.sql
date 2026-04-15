with 

source as (
    select *
    from {{source('olist', 'translation')}}
)

select 
   string_field_0 as product_category_name,
   string_field_1 as english_translation
from source

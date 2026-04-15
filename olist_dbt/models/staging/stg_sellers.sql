with 

source as(
    select *
    from {{source('olist', 'sellers')}}
)

select 
    seller_id,
    seller_zip_code_prefix as seller_zip,
    seller_city,
    seller_state
from source

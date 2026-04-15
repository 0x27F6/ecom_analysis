with 

source as(
    select *
    from {{source('olist', 'payments')}}
)

select *
from source 

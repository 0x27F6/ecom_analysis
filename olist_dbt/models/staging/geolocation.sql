with 

source as(
    select *
    from {{ref('olist_geolocation_dataset')}}
)

select 
    geolocation_zip_code_prefix as zip_code,
    geolocation_lat as lat,
    geolocation_lng as lon,
    geolocation_city as city,
    geolocation_state as state
from source
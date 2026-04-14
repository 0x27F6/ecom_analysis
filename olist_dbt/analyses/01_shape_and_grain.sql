select 
    'sellers' as table_name,
    count(*) as row_count,
    count(distinct seller_id) as pk_distinct,
    null as pk2_distinct,
    null as pk3_distinct,
    null as pk4_distinct,
    null as pk_combo_distinct
from {{ref('sellers')}}

union all

select 
    'products' as table_name,
    count(*) as row_count,
    count(distinct product_id) as pk_distinct,
    null as pk2_distinct,
    null as pk3_distinct,
    null as pk4_distinct,
    null as pk_combo_distinct
from {{ref('products')}}

union all

select 
    'payments' as table_name,
    count(*) as row_count,
    count(distinct order_id) as pk_distinct,
    null as pk2_distinct,
    null as pk3_distinct,
    null as pk4_distinct,
    count(distinct concat(order_id, '-', payment_sequential)) as pk_combo_distinct
from {{ref('payments')}}

union all

select 
    'orders' as table_name,
    count(*) as row_count,
    count(distinct order_id) as pk_distinct,
    count(distinct customer_id) as pk2_distinct,
    null as pk3_distinct,
    null as pk4_distinct,
    count(distinct concat(order_id, '-', customer_id)) as pk_combo_distinct
from {{ref('orders')}}

union all

select 
    'order_reviews' as table_name,
    count(*) as row_count,
    count(distinct review_id) as pk_distinct,
    count(distinct order_id) as pk2_distinct,
    null as pk3_distinct,
    null as pk4_distinct,
    count(distinct concat(review_id, '-', order_id)) as pk_combo_distinct
from {{ref('order_reviews')}}

union all 

select
    'items' as table_name,
    count(*) as row_count,
    count(distinct order_id) as pk_distinct,
    count(distinct order_item_id) as pk2_distinct,
    count(distinct product_id) as pk3_distinct,
    count(distinct seller_id) as pk4_distinct,
    count(distinct concat(order_id, '-', order_item_id))
from {{ref('items')}}

union all

select 
    'customers' as table_name,
    count(*) as row_count,
    count(distinct customer_id) as pk_distinct,
    count(distinct unique_id) as pk2_distinct,
    null as pk3_distinct,
    null as pk4_distinct,
    count(distinct concat(customer_id, '-', unique_id)) as pk_combo_distinct
from {{ref('customers')}}

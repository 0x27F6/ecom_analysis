-- goal: collapse payments to the order grain. 
-- one row per order_id. currently payments can be split
with 

/* I am going to roll these up so theres one payment per order id. 
if there's multiple payments -> use a case to label as split order else payment_type 
*/
payments as (
    select *
    from {{ref('stg_payments')}}
),

valid_payments as (
    -- remove invalid labels BEFORE any aggregation logic
    select *
    from payments
    where payment_type != 'not_defined'
)

select
   order_id,

    -- order-level counts
    count(*) as payment_count,
    round(sum(payment_value), 2) as total_paid,

    -- dominant payment type (now only valid values compete)
    max_by(payment_type, payment_value) as primary_payment_type,

    -- classification logic
    case 
        when count(*) > 1 then 'split'
        else max(payment_type)
    end as payment_method

from valid_payments
group by order_id 
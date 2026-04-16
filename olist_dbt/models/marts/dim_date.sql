with date_spine as (
    select date_day
    from unnest(
        generate_date_array('2016-01-01', '2019-12-31', interval 1 day)
    ) as date_day
)

select
    date_day,
    extract(dayofweek from date_day) as day_of_week,
    extract(day from date_day) as day_of_month,
    extract(dayofyear from date_day) as day_of_year,
    extract(week from date_day) as week_of_year,
    extract(month from date_day) as month_of_year,
    format_date('%B', date_day) as month_name,
    extract(quarter from date_day) as quarter_of_year,
    extract(year from date_day) as year_number,
    case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend
from date_spine
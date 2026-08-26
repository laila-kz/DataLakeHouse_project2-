with date_range as (

    select cast(range as date) as date_day
    from range(DATE '2019-01-01', DATE '2028-01-01', INTERVAL 1 DAY)

),


calculated as (

    select
        cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
        date_day,
        dayofweek(date_day) as day_of_week,
        strftime(date_day, '%A') as day_name,
        month(date_day) as month,
        strftime(date_day, '%B') as month_name,
        quarter(date_day) as quarter,
        year(date_day) as year,
        case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
    from date_range

)

select * from calculated

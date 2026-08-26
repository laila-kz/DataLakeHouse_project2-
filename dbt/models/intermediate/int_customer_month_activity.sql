with customer_cohorts as (

    select
        customer_key,
        user_id,
        date_trunc('month', cast(min(purchased_at) as date)) as cohort_month
    from {{ ref('fact_purchases') }}
    group by customer_key, user_id

),

month_spine as (

    select distinct
        date_trunc('month', date_day) as activity_month
    from {{ ref('dim_date') }}
    where date_day <= (select cast(max(purchased_at) as date) from {{ ref('fact_purchases') }})
      and date_day >= (select cast(min(purchased_at) as date) from {{ ref('fact_purchases') }})

),

customer_month_grid as (

    select
        c.customer_key,
        c.user_id,
        c.cohort_month,
        m.activity_month,
        (
            (extract(year from m.activity_month) - extract(year from c.cohort_month)) * 12 +
            (extract(month from m.activity_month) - extract(month from c.cohort_month))
        ) as months_since_first_purchase
    from customer_cohorts c
    cross join month_spine m
    where m.activity_month >= c.cohort_month

),

monthly_purchases as (

    select
        customer_key,
        date_trunc('month', cast(purchased_at as date)) as activity_month,
        count(*) as purchase_count
    from {{ ref('fact_purchases') }}
    group by customer_key, date_trunc('month', cast(purchased_at as date))

),

final as (

    select
        g.customer_key,
        g.user_id,
        g.cohort_month,
        g.activity_month,
        g.months_since_first_purchase,
        coalesce(p.purchase_count, 0) as purchase_count,
        case when coalesce(p.purchase_count, 0) > 0 then true else false end as made_purchase
    from customer_month_grid g
    left join monthly_purchases p
        on g.customer_key = p.customer_key
        and g.activity_month = p.activity_month

)

select * from final

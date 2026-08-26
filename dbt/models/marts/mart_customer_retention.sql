with activity as (

    select * from {{ ref('int_customer_month_activity') }}

),

cohort_sizes as (

    select
        cohort_month,
        count(distinct customer_key) as cohort_size
    from activity
    where months_since_first_purchase = 0
    group by cohort_month

),

retention_counts as (

    select
        cohort_month,
        months_since_first_purchase,
        count(distinct case when made_purchase then customer_key end) as retained_count
    from activity
    group by cohort_month, months_since_first_purchase

),

final as (

    select
        r.cohort_month,
        r.months_since_first_purchase,
        s.cohort_size,
        r.retained_count,
        round(cast(r.retained_count as double) / nullif(s.cohort_size, 0), 4) as retention_rate
    from retention_counts r
    inner join cohort_sizes s
        on r.cohort_month = s.cohort_month

)

select * from final

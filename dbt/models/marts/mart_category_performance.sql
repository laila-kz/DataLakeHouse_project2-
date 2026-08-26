with category_monthly as (

    select
        coalesce(p.category_l1, 'unknown') as category_l1,
        date_trunc('month', cast(f.purchased_at as date)) as period,
        count(distinct f.purchase_key) as total_orders,
        coalesce(sum(f.revenue), 0.0) as total_revenue
    from {{ ref('fact_purchases') }} f
    left join {{ ref('dim_product') }} p
        on f.product_key = p.product_key
    group by 1, 2

),

with_prior as (

    select
        category_l1,
        period,
        total_orders,
        total_revenue,
        lag(total_revenue) over (partition by category_l1 order by period) as prior_period_revenue,
        lag(total_orders) over (partition by category_l1 order by period) as prior_period_orders
    from category_monthly

),

final as (

    select
        category_l1,
        period,
        total_orders,
        total_revenue,
        prior_period_revenue,
        prior_period_orders,
        round(cast((total_revenue - prior_period_revenue) as double) / nullif(prior_period_revenue, 0), 4) as revenue_growth_rate,
        case
            when prior_period_revenue is null then 'new_category'
            when prior_period_revenue = 0 then 'no_prior_revenue'
            else 'comparable'
        end as growth_rate_label
    from with_prior

)

select * from final

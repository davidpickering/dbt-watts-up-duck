with

power as (
    select * from {{ ref('fact_circuit_power') }}
),

agg_daily as (
    select
        date_trunc('day', logged_at) as logged_date,
        sum(power_kw) as total_energy_kwh,
        sum(case when circuit = 'garage_shop_panels' then power_kw else 0 end) as garage_shop_panels,
        sum(case when circuit = 'heat_pump' then power_kw else 0 end) as heat_pump,
        sum(case when circuit = 'air_handler' then power_kw else 0 end) as air_handler,
        sum(case when circuit = 'fridges' then power_kw else 0 end) as fridges

    from power
    where circuit NOT IN ('mains_a', 'mains_b', 'mains_c')
    group by 1
),

final as (
    select
        logged_date,
        round(total_energy_kwh, 3) as total_energy,
        round(garage_shop_panels, 3) as garage_shop_panels,
        round(heat_pump, 3) as heat_pump,
        round(air_handler, 3) as air_handler,
        round(fridges, 3) as fridges
    from agg_daily
)

select * from final order by 1 desc

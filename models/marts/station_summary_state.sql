with

stg_dcfast as (
        select * from {{ref('stg_dcfast__charging_stations_usa')}} where length(state) = 2
),

agg_dcfast as (
        select
                state,
                count(*) as num_stations,
                sum(ports) as total_ports,
                min(power_kw) as min_power_kw,
                max(power_kw) as max_power_kw,
                sum(is_fast_dc) as num_fast_dc
        from stg_dcfast
        group by 1
)

select * from agg_dcfast
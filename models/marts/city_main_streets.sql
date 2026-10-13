with

water_meters as (
    select * from {{ ref('stg_geocoding__water_meters') }}
),

street_counts as (
    select
        city,
        -- concat_ws skips nulls, so streets with no type (e.g. "Broadway") still show
        concat_ws(' ', address_street_name, address_street_type) as main_street,
        count(*) as water_meters
    from water_meters
    where address_street_name is not null
    group by 1, 2
),

final as (
    select
        *
    from street_counts
    -- QUALIFY filters on a window function directly, no subquery needed.
    -- Street name breaks ties so the result is deterministic.
    qualify row_number() over (
        partition by city
        order by water_meters desc, main_street
    ) = 1
)

select * from final
order by water_meters desc

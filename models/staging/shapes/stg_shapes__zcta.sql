with

source as (
    select * from {{ source('shapes', 'zcta') }}
),

renamed as (
    select
        {{adapter.quote("ZCTA5CE20")}}::varchar as zcta,
        {{adapter.quote("GEOID20")}}::varchar as geoid,
        {{adapter.quote("GEOIDFQ20")}}::varchar as geoid_full_qualified,
        {{adapter.quote("CLASSFP20")}}::varchar as class_fips,
        {{adapter.quote("MTFCC20")}}::varchar as feature_class_code,
        {{adapter.quote("FUNCSTAT20")}}::varchar as functional_status,
        {{adapter.quote("ALAND20")}}::bigint as area_land_sq_meters,
        {{adapter.quote("AWATER20")}}::bigint as area_water_sq_meters,
        {{adapter.quote("INTPTLAT20")}}::double as internal_point_lat,
        {{adapter.quote("INTPTLON20")}}::double as internal_point_lon,
        {{adapter.quote("geom")}}::GEOMETRY as geom,
        ST_Point(internal_point_lon, internal_point_lat) as geom_point
    from source
),

final as (
    select
        {{dbt_utils.generate_surrogate_key(['zcta'])}} as zcta_sk,
        renamed.*
    from
        renamed
)

select * from final
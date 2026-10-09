with

source as (
    select * from {{ source('shapes', 'states') }}
),

renamed as (
    select
        {{adapter.quote("REGION")}}::varchar as region,
        {{adapter.quote("DIVISION")}}::varchar as division,
        {{adapter.quote("STATEFP")}}::varchar as state_fips,
        {{adapter.quote("STATENS")}}::varchar as state_ansi,
        {{adapter.quote("GEOID")}}::varchar as geoid,
        {{adapter.quote("GEOIDFQ")}}::varchar as geoid_full_qualified,
        {{adapter.quote("STUSPS")}}::varchar as state_abbreviation,
        {{adapter.quote("NAME")}}::varchar as name,
        {{adapter.quote("LSAD")}}::varchar as legal_statistical_area_desc,
        {{adapter.quote("MTFCC")}}::varchar as feature_class_code,
        {{adapter.quote("FUNCSTAT")}}::varchar as functional_status,
        {{adapter.quote("ALAND")}}::bigint as area_land_sq_meters,
        {{adapter.quote("AWATER")}}::bigint as area_water_sq_meters,
        {{adapter.quote("INTPTLAT")}}::double as internal_point_lat,
        {{adapter.quote("INTPTLON")}}::double as internal_point_lon,
        {{adapter.quote("geom")}}::GEOMETRY as geom,
        ST_Point(internal_point_lon, internal_point_lat) as geom_point
    from source
),

final as (
    select
        {{dbt_utils.generate_surrogate_key(['state_abbreviation'])}} as state_sk,
        renamed.*
    from
        renamed
)

select * from final
with

source as (
    select * from {{ source('shapes', 'counties') }}
),

renamed as (
    select
        {{adapter.quote("STATEFP")}}::varchar as state_fips,
        {{adapter.quote("COUNTYFP")}}::varchar as county_fips,
        {{adapter.quote("COUNTYNS")}}::varchar as county_ansi,
        {{adapter.quote("GEOID")}}::varchar as geoid,
        {{adapter.quote("GEOIDFQ")}}::varchar as geoid_full_qualified,
        {{adapter.quote("NAME")}}::varchar as name,
        {{adapter.quote("NAMELSAD")}}::varchar as name_lsad,
        {{adapter.quote("LSAD")}}::varchar as legal_statistical_area_desc,
        {{adapter.quote("CLASSFP")}}::varchar as class_fips,
        {{adapter.quote("MTFCC")}}::varchar as feature_class_code,
        {{adapter.quote("CSAFP")}}::varchar as csa_fips,
        {{adapter.quote("CBSAFP")}}::varchar as cbsa_fips,
        {{adapter.quote("METDIVFP")}}::varchar as metro_division_fips,
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
        {{dbt_utils.generate_surrogate_key(['geoid'])}} as county_sk,
        renamed.*
    from
        renamed
)

select * from final
with

water_meters as (
    select * from {{ ref('stg_geocoding__water_meters') }}
    -- a handful of address points have a blank zip
    where nullif(trim(zip), '') is not null
),

zip_counts as (
    select
        zip,
        count(*) as water_meters,
        count(distinct city) as cities,
        count(distinct concat_ws(' ', address_street_name, address_street_type)) as streets,
        count(distinct parcel_pin) as parcels,
        -- mode() returns the most common value, handy for zips that span cities
        mode(city) as primary_city,
        -- rough center of the zip, ignoring points with missing coordinates
        avg(nullif(latitude, 0)) as center_latitude,
        avg(nullif(longitude, 0)) as center_longitude
    from water_meters
    group by zip
),

final as (
    select
        *,
        round(100 * water_meters / sum(water_meters) over (), 2) as pct_of_water_meters
    from zip_counts
)

select * from final
order by water_meters desc

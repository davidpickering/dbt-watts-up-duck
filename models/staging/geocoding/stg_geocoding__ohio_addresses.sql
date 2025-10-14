with

source as (
    select * from {{ source('geocoding', 'ohio_geocoded_addresses') }}
),

renamed as (
    select
        station_id,
        latitude,
        longitude,
        street_address as reverse_geocoded_address_raw,
        station_name,
        city,
        state,
        zip_code,
        geocoded_at,
        geocoding_source
    from source
)

select * from renamed

-- TODO additional work here to parse the reverse geocoded address into components
-- -- It looks like we might have "reverse geocoded" to nearest point of interest rather than an actual address
-- -- e.g., 13999, SR 2, Grodis Corner, Benton Township, Ottawa County, Ohio, 43449, United States
-- -- e.g., Walgreens, East Broad Street, Columbus, Franklin County, Ohio, 43209, United States

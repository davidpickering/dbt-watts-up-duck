with

addresses as (
    select * from {{ ref('dim_addresses') }}
),

state_data as (
    select * from {{ ref('stg_shapes__states') }}
),

enriched as (
    
    SELECT
        addresses.latitude_best as latitude,
        addresses.longitude_best as longitude,
        ST_AsGeoJSON(ST_Point(addresses.longitude_best, addresses.latitude_best)) as geom_point,
        state_data.NAME as state_name,
        st_x(ST_Centroid(state_data.geom)) as state_centroid_lon,
        st_y(ST_Centroid(state_data.geom)) as state_centroid_lat,
        ST_AsGeoJSON(ST_Envelope(state_data.geom)) as state_bounding_box
    FROM addresses
    JOIN state_data
        ON ST_Within(ST_Point(addresses.longitude_best, addresses.latitude_best), state_data.geom)
    WHERE addresses.latitude_best IS NOT NULL 
    AND addresses.longitude_best IS NOT NULL
),

final as (
    select
    
        {{ dbt_utils.generate_surrogate_key([
            'latitude',
            'longitude'
            ]) }} as dim_geo_sk,
    latitude,
    longitude,
    geom_point,
    state_name,
    state_centroid_lon,
    state_centroid_lat,
    state_bounding_box
    from enriched
)

select * from final

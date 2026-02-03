with

addresses as (
    select * from {{ ref('dim_addresses') }}
),

state_data as (
    select * from {{ ref('stg_shapes__states') }}
),

final as (
    
    SELECT
        addresses.latitude_best,
        addresses.longitude_best,
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
    LIMIT 500

)

select * from final

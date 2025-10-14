{{
  config(
    materialized='table',
    description='Address dimension with H3 spatial indexing'
  )
}}

with

charging_stations as (
    select * from {{ ref('stg_dcfast__charging_stations_usa') }}
),

charge_points as (
    select * from {{ ref('stg_paren__charge_points') }}
),


union_addresses as (
        select
            street_address_1,
            street_address_2,
            city,
            state,
            zip,
            address_sk,
            latitude,
            longitude
        from
        charging_stations
    union all
        select
            street_address_1,
            street_address_2,
            city,
            state,
            zip,
            address_sk,
            NULL as latitude,
            NULL as longitude
        from
        charge_points
),

unique_addresses as (
    select 
        street_address_1,
        street_address_2,
        city,
        state,
        zip,
        address_sk,
        max(
            case
                when latitude is not null and longitude is not null then latitude
                when latitude <> 0 and longitude <> 0 then latitude
                end
                ) as latitude_best,
        max(
            case
                when latitude is not null and longitude is not null then longitude
                when latitude <> 0 and longitude <> 0 then longitude
            end ) as longitude_best,
    from union_addresses
    group by 1, 2, 3, 4, 5, 6
),

final as (
    select 
        *,
        -- Convert lat/long to H3 cell at resolution 8
        case 
            when latitude_best is not null and longitude_best is not null then
                h3_latlng_to_cell(latitude_best, longitude_best, 8)
            else null
        end as h3_cell_resolution_8,
        
        -- Add H3 cell metadata
        case 
            when latitude_best is not null and longitude_best is not null then
                h3_cell_to_parent(h3_latlng_to_cell(latitude_best, longitude_best, 8), 7)
            else null
        end as h3_cell_resolution_7,
        
        case 
            when latitude_best is not null and longitude_best is not null then
                h3_cell_to_parent(h3_latlng_to_cell(latitude_best, longitude_best, 8), 6)
            else null
        end as h3_cell_resolution_6
        
    from unique_addresses
    )

select * from final
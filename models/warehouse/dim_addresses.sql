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
    select * from unique_addresses
    )

select * from final
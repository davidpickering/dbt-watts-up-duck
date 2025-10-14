with

source as (
    select * from {{ source('paren', 'paren_qsr_sample') }}
),

final as (
    select
        {{adapter.quote("Network")}} as network,
        {{adapter.quote("Street")}} as street_raw,
        {{adapter.quote("City")}} as city_raw,
        {{adapter.quote("State")}} as state_raw,
        {{adapter.quote("Zip Code")}} as zip_code_raw,
        {{standardize_address('street')}} as street_address_1,
        NULL as street_address_2,
        {{standardize_address('city_raw')}} as city,
        {{standardize_address('state_raw')}} as state,
        zip_code_raw as zip,
        {{ dbt_utils.generate_surrogate_key([
            'street_address_1',
            'street_address_2',
            'city',
            'state',
            'zip'
            ]) }} as address_sk,
        {{adapter.quote("Ports")}} as charger_ports,
        "Max Power (kW)" as power_kw_max,
        "Min Power (kW)" as power_kw_min,
        {{ dbt_utils.generate_surrogate_key(['power_kw_max', 'power_kw_min']) }} as power_min_max_sk,
        {{adapter.quote("QSR Name")}} as nearby_fast_food,
        {{adapter.quote("Walking Distance (meter)")}} as distance_fast_food_meters
        
    from
        source
)

{# select {{ dbt_utils.generate_surrogate_key(['Network', 'address_sk']) }} as chargepoint_sk, * from final --bad SK example #}
select {{ dbt_utils.generate_surrogate_key(['Network', 'address_sk', 'charger_ports', 'power_min_max_sk', 'nearby_fast_food', 'distance_fast_food_meters']) }} as chargepoint_sk, * from final

-- Applegreen,Route 15 Northbound,North Haven,CT,6473,4,150,100,Dunkin',35
-- Applegreen,Route 15 Northbound,North Haven,CT,6473,4,150,100,Dunkin',37
-- Same network, address, restaruant, power, etc... but different distance to restaurant - might be a data issue, 2 sets of chargers across the road from each other?
-- https://maps.app.goo.gl/31PyDeR8Rc3Nkbxa7

-- Charge Point,1827 Griffith Dr,Harriman,TN,37748,3,400,400,Dunkin',35
-- Charge Point,1827 Griffith Dr,Harriman,TN,37748,1,400,400,Dunkin',35
-- Edge case - same network, address, restaurant, distance, power, etc... but classified as 1-3 port installation, and 1-1 port installation
-- https://www.google.com/maps/place/ChargePoint+Charging+Station/@35.889928,-84.5479166,3a,75y,90t/data=!3m8!1e2!3m6!1sCIABIhAt_goJKcyJOOLBcTEKY9_i!2e10!3e12!6shttps:%2F%2Flh3.googleusercontent.com%2Fgps-cs-s%2FAC9h4nomK_8decH-oZ8t8LisPTWkRC21vpJz24Uk55VonFCICjRGTuxE2sb0whCbBlghEY8VWyFXc7EzxHuVDVL0CfHBPzNG6BmL4VvvjE1CusIcYyb2dDhbnw8TC0X6v0YsdhycoQzt3hQbuGg%3Dw114-h86-k-no!7i1710!8i1284!4m16!1m8!3m7!1s0x885ddc3ca9432a7f:0x149eebf30308779c!2s1827+Griffith+Dr,+Harriman,+TN+37748!3b1!8m2!3d35.8901283!4d-84.5481716!16s%2Fg%2F11nnkqzfdm!3m6!1s0x885ddc3cad76870b:0xbed83020598c3b2a!8m2!3d35.889889!4d-84.547832!10e5!16s%2Fg%2F11x5n89dhg?entry=ttu&g_ep=EgoyMDI1MTAwOC4wIKXMDSoASAFQAw%3D%3D
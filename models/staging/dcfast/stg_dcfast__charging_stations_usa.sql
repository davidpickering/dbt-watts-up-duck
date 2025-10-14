with

source as (
    select * from {{ source('dcfast', 'world_charging_stations') }} where country_code = 'US'
),

renamed as (
    select
        {{adapter.quote("id")}}::integer as id,
        {{adapter.quote("name")}}::text as network_name,
        NULL as street_address_1,
        NULL as street_address_2,
        {{adapter.quote("city")}}::text as city,
        {{adapter.quote("country_code")}}::text as country_code,
        {{adapter.quote("state_province")}}::text as state,
        {# upper({{adapter.quote("state_province")}}::text) as state, #} --demonstrate cleanup in staging benefiting marts
        NULL as zip,
        CAST({{adapter.quote("latitude")}} AS DECIMAL(9,6)) as latitude,
        CAST({{adapter.quote("longitude")}} AS DECIMAL(10,7)) as longitude,
        {{adapter.quote("ports")}}::integer as ports,
        {{adapter.quote("power_kw")}} as power_kw_raw,
        {{adapter.quote("power_kw")}}::integer as power_kw,
        {{adapter.quote("power_class")}}::text as power_class_raw,
        {{adapter.quote("is_fast_dc")}}::boolean as is_fast_dc
    from source

),

final as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'street_address_1',
            'street_address_2',
            'city',
            'state',
            'zip'
            ]) }} as address_sk,
        CASE
            WHEN power_class_raw LIKE 'AC_%' THEN 'AC'
            WHEN power_class_raw LIKE 'DC_%' THEN 'DC'
            WHEN power_class_raw = 'UNKNOWN' THEN 'UNKNOWN'
            ELSE 'UNKNOWN'
        END AS current_type,
          CASE
            WHEN power_class_raw = 'AC_L1_(<7.5kW)' THEN 0
            WHEN power_class_raw = 'AC_L2_(7.5-21kW)' THEN 7.5
            WHEN power_class_raw = 'AC_HIGH_(22-49kW)' THEN 22
            WHEN power_class_raw = 'DC_FAST_(50-149kW)' THEN 50
            WHEN power_class_raw = 'DC_ULTRA_(>=150kW)' THEN 150
            ELSE NULL
        END AS power_kw_low,
        CASE
            WHEN power_class_raw = 'AC_L1_(<7.5kW)' THEN 7.5
            WHEN power_class_raw = 'AC_L2_(7.5-21kW)' THEN 21
            WHEN power_class_raw = 'AC_HIGH_(22-49kW)' THEN 49
            WHEN power_class_raw = 'DC_FAST_(50-149kW)' THEN 149
            WHEN power_class_raw = 'DC_ULTRA_(>=150kW)' THEN 600  -- open-ended, no upper limit
            ELSE NULL
        END AS power_kw_high
    from
        renamed
)

select * from final
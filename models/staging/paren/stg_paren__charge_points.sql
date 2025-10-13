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
        "Min Power (kW)" as power_kw_min


    from
        source
)

select {{ dbt_utils.generate_surrogate_key(['Network', 'address_sk']) }} as chargepoint_sk, * from final
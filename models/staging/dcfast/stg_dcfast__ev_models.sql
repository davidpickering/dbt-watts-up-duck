with

source as (
    select * from {{ source('dcfast', 'ev_models_2025') }}
    --select * from {{ source('dcfast', 'ev_models_2025') }} where market_regions LIKE '%US%'
),

renamed as (
    select
        {{adapter.quote("make")}}::text as make,
        {{adapter.quote("model")}}::text as model,
        {{adapter.quote("market_regions")}}::text as market_regions,
        {{adapter.quote("powertrain")}}::text as powertrain,
        {{adapter.quote("first_year")}}::integer as first_year,
        {{adapter.quote("body_style")}}::text as body_style,
        {{adapter.quote("origin_country")}}::text as origin_country
    from source

)

select * from renamed
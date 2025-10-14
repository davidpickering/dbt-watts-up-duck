with

source as (
    select * from {{ source('paren', 'ev_models_charge_capability') }}
),

renamed as (
    select
        {{adapter.quote("make")}}::text as make,
        {{adapter.quote("model")}}::text as model,
        {{adapter.quote("first_year")}}::integer as first_year,
        {{adapter.quote("onboard_charger_kw")}}::float as l2_charger_kw,
        {{adapter.quote("dc_fast_charge_max_kw")}}::float as dc_fast_kw
    from source
),

final as (
    select
        {{generate_surrogate_key('make', 'model', 'first_year')}} as ev_model_sk
        make,
        model,
        first_year,
        round(l2_charger_kw, 1) as l2_charger_kw,
        round(dc_fast_kw, 1) as dc_fast_kw
    from
        renamed
)

select * from final
with

ev_models as (
    select * from {{ ref('stg_dcfast__ev_models') }}
),

ev_capability as (
    select * from {{ ref('stg_paren__ev_capability') }}
),

joined as (
    select
        ev_models.ev_model_sk,
        ev_models.make,
        ev_models.model,
        ev_models.market_regions,
        ev_models.powertrain,
        ev_models.first_year,
        ev_models.body_style,
        ev_models.origin_country,
        ev_capability.l2_charger_kw,
        ev_capability.dc_fast_kw
    from ev_models
    left join ev_capability
        on ev_models.ev_model_sk = ev_capability.ev_model_sk

),

-- original join
-- -- left join ev_capability
-- -- on ev_models.make = ev_capability.make
-- -- and ev_models.model = ev_capability.model
-- -- and ev_models.first_year = ev_capability.first_year

final as (
    select
        make,
        model,
        market_regions,
        powertrain,
        first_year,
        body_style,
        origin_country,
        l2_charger_kw,
        dc_fast_kw,
        ev_model_sk
    from joined
)

select * from final

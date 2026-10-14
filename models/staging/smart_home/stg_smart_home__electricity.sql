with

source as (
    select * from {{ source('smart_home', 'electricity_usage_1_hour') }}
),

renamed as (
    select
        {# Time bucket arrives as text, e.g. '10/05/2026 22:00:00', in America/New_York local time #}
        strptime({{adapter.quote("Time Bucket (America/New_York)")}}, '%m/%d/%Y %H:%M:%S') as reading_at_local,

        {# Mains (whole-house legs). Mains_C has no sensor ('No CT'), so try_cast turns it into null #}
        {{adapter.quote("Main Panel (200A) Mains_A (kWhs)")}}::double as mains_a_kw,
        {{adapter.quote("Main Panel (200A) Mains_B (kWhs)")}}::double as mains_b_kw,
        {# try_cast({{adapter.quote("Main Panel (200A) Mains_C (kWhs)")}} as double) as mains_c_kw, #} -- No C Leg to monitor

        {# Heating and cooling #}
        {{adapter.quote("Main Panel (200A) Heat Pump-Heat Pump 2 (14 & 16) [Redx2] (kWhs)")}}::double as heat_pump_kw,
        {{adapter.quote("Main Panel (200A) Furnace-Air Handler (18 & 20) [White/Redx2] (kWhs)")}}::double as air_handler_kw,
        {{adapter.quote("Main Panel (200A) Air Conditioner-second Bedroom - AC Outlet (9) (kWhs)")}}::double as second_bedroom_ac_kw,
        {{adapter.quote("Main Panel (200A) Air Conditioner-Mstr Bed - AC Outlet (4) (kWhs)")}}::double as master_bedroom_ac_kw,

        {# Kitchen #}
        {{adapter.quote("Main Panel (200A) Fridge/Freezer-Fridges (15) (kWhs)")}}::double as fridges_kw,
        {{adapter.quote("Main Panel (200A) Kitchen-Kitchen GFCI (22) (kWhs)")}}::double as kitchen_gfci_kw,
        {{adapter.quote("Main Panel (200A) Kitchen-Kitchen Utils (24) (kWhs)")}}::double as kitchen_utils_kw,
        {{adapter.quote("Main Panel (200A) Cooktop/Range/Oven/Stove-Range (38 & 40) (kWhs)")}}::double as range_kw,
        {{adapter.quote("Main Panel (200A) Lights-Kitchen Lights (1) (kWhs)")}}::double as kitchen_lights_kw,

        {# Laundry and water #}
        {{adapter.quote("Main Panel (200A) Clothes Dryer-Dryer (27 & 29) (kWhs)")}}::double as dryer_kw,
        {{adapter.quote("Main Panel (200A) Clothes Washer-Basement + Washing Machine (31) (kWhs)")}}::double as basement_washer_kw,
        {{adapter.quote("Main Panel (200A) Water Heater-Water Heater (34 & 36) (kWhs)")}}::double as water_heater_kw,

        {# Rooms and multi-use circuits #}
        {{adapter.quote("Main Panel (200A) Room/Multi-use Circuit-Dining GFCI (32) (kWhs)")}}::double as dining_gfci_kw,
        {{adapter.quote("Main Panel (200A) Room/Multi-use Circuit-Basement + Internet (11) (kWhs)")}}::double as basement_internet_kw,

        {# Sub panels #}
        {{adapter.quote("Main Panel (200A) Sub Panel-Generator Panel (33 & 35) (kWhs)")}}::double as generator_panel_kw,
        {{adapter.quote("Main Panel (200A) Sub Panel-Garage + Shop Panels (37 & 39) (kWhs)")}}::double as garage_shop_panels_kw
    from source
)

select * from renamed order by reading_at_local asc

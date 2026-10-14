with

source as (
    select * from {{ source('geocoding', 'cle_open_data_water_meters') }}
),

renamed as (
    select
        objectid as point_id,
        addr_num as address_number,
        pre_dir as address_pre_directional,
        str_name as address_street_name,
        str_type as address_street_type,
        suf_dir as address_suffix_direction,
        {# addr_source, #}
        {# cat_name, #}
        address as street_address_1,
        coalesce(building, unit, '') as street_address_2,
        city,
        state,
        zip,
        {{ dbt_utils.generate_surrogate_key([
            'street_address_1',
            'street_address_2',
            'city',
            'state',
            'zip'
            ]) }} as address_sk,

        {# info #}
        parcelpin as parcel_pin,
        county as county_abbreviation,
        cnty_fips as county_code,
        muni_fips as fips_municipal,

        {# Coordinates #}
        lat_y as latitude,
        long_x as longitude,

        {# metadata #}
        {# createdby, #}
        createddate as added_at,
        {# modifiedby, #}
        modifieddate as updated_at,

        {#  #}
        {# addressid, #}
        {# commonplacename, #}
        {# addressid_txt, #}
        point_type,
        {# created_user, #}
        {# created_date, #}
        {# last_edited_user, #}
        last_edited_date,
        building,
        unit,
        globalId as guuid
    from source
),

final as (
    select
        *
    from renamed
)

select * from final
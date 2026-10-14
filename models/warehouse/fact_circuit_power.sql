with

tall_data as (
    {{ dbt_utils.unpivot(
        relation=ref('stg_smart_home__electricity'),
        cast_to='double',
        exclude=['reading_at_local'],
        field_name='circuit',
        value_name='power_kw'
    ) }}
),


final as (
    select
        reading_at_local as logged_at,
        replace(circuit, '_kw', '') as circuit,
        {{ dbt_utils.generate_surrogate_key([
            'logged_at',
            'circuit'
            ]) }} as time_circuit_sk,
        power_kw
    from tall_data
)

select * from final order by 2, 1
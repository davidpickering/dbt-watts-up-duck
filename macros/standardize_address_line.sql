{% macro standardize_address_1_2(column_name, field) -%}
    {%- if field == 'address_1' -%}
        upper(
            ltrim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            {{ column_name }},
                            '[^a-zA-Z0-9\s]', ' '  -- Replace special characters with spaces
                        ),
                        ' +', ' '  -- Replace multiple spaces with single space
                    ),
                    ' $', ''  -- Remove trailing spaces
                )
            )
        )
    {%- elif field == 'address_2' -%}
        case
            when {{ column_name }} is null then null
            when {{ column_name }} = '' then null
            else upper(
                rtrim(
                    ltrim(
                        regexp_replace(
                            regexp_replace(
                                {{ column_name }},
                                '  +', ' '  -- Replace multiple spaces with single space
                            ),
                            ' $', ''  -- Remove trailing spaces
                        )
                    )
                )
            )
        end
    {%- else -%}
        'error'
    {%- endif -%}
{%- endmacro %}
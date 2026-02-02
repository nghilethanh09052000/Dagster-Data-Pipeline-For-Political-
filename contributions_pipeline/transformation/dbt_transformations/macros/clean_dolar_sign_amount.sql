{% macro clean_dolar_sign_amount(column_name) %}
   cast(
        case
            when {{ column_name }} ~ '^\(.*\)$' then
                '-' || regexp_replace({{ column_name }}, '[$,()]', '', 'g')
            else
                regexp_replace({{ column_name }}, '[$,]', '', 'g')
        end
        as numeric
    )
{% endmacro %}

{% macro generate_committee_id(source_literal, name_column) %}
    {{ "'" ~ source_literal ~ "'" }} || {{ dbt_utils.generate_surrogate_key([name_column]) }}
{% endmacro %}

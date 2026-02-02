{% macro parse_flexible_date(date_column) %}
  CASE 
    WHEN {{ date_column }} ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' 
      THEN to_timestamp({{ date_column }}, 'MM/DD/YYYY')
    WHEN {{ date_column }} ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$' 
      THEN to_timestamp({{ date_column }}, 'YYYY-MM-DD')
    WHEN {{ date_column }} ~ '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}T[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}' 
      THEN to_timestamp({{ date_column }}, 'YYYY-MM-DD"T"HH24:MI:SS')
    ELSE NULL
  END
{% endmacro %}

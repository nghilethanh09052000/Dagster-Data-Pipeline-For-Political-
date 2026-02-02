{% test assert_has_prefix(model, column_name, prefix) %}

SELECT
    {{ column_name }}
FROM
    {{ model }}
WHERE
    {{ column_name }} IS NOT NULL
    AND NOT {{ column_name }} LIKE '{{ prefix }}%'

{% endtest %}

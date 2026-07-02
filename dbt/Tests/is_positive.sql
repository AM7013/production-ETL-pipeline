{% test is_positive(model, column_name) %}
    SELECT {{ column_name }}::numeric
    FROM {{ model }}
    WHERE {{ column_name }}::text ~ '^-?\d+(\.\d+)?$'
    AND {{ column_name }}::numeric <= 0
{% endtest %}

{% macro percentage_calc(numerator, denominator) %}
    {% if denominator == 0 %}
        0
    {% else %}
        case when {{numerator}}::text ~ '^[0-9]+\.?[0-9]*$' and {{denominator}}::text ~ '^[0-9]+\.?[0-9]*$' then
                ({{numerator}}::float / {{denominator}}::float) * 100
            else
                null
            end
    {% endif %}

{% endmacro %}
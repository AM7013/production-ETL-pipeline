{% macro tax_calc(price_col, qty_col, rate) %}

    {% if price_col is number and qty_col is number %}
        {{ price_col * qty_col * rate / 100 }}

    {% else %}
            (
                case
                    when {{price_col}}::text ~ '^[0-9]+\.?[0-9]*$' and {{qty_col}}::text ~ '^[0-9]+\.?[0-9]*$' then
                        ({{price_col}}::float * {{qty_col}}::float * {{rate}} / 100)
                    else
                        null
                end
            )
    {% endif %}
{% endmacro %}
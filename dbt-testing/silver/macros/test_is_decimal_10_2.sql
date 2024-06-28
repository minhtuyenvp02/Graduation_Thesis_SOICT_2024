{% macro test_is_decimal_10_2(model, column_name) %}

    select
        count(*)
    from {{ model }}
    where not (try_cast({{ column_name }} as decimal(10,2)) is not null)

{% endmacro %}

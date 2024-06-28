{% macro test_is_iso8601_timestamp(model, column_name) %}

    select
        count(*)
    from {{ model }}
    where not (regexp_like(CAST({{ column_name }} AS varchar), '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{2}:\\d{2}|Z)$'))

{% endmacro %}

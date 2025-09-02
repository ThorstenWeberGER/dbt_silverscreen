-- This generic test checks if a date column has values earlier than threshold
-- set threshold in dbt_project.yml

{% test not_earlier_than_threshold_date(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < '{{ var('threshold_date') }}'

{% endtest %}

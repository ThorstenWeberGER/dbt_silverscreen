-- tests/generic/not_earlier_than_threshold_date.sql

{% test not_earlier_than_threshold_date(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < '{{ var('threshold_date') }}'

{% endtest %}

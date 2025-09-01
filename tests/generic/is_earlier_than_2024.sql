-- This generic test checks if a date column has values earlier than 2024-01-01
-- It is designed to be reusable across multiple models and columns.

{% test is_earlier_than_2024(model, column) %}

select
    {{ column }}
from {{ model }}
where {{ column }} < '2024-01-01'

{% endtest %}

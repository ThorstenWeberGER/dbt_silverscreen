-- This generic test checks if a column contains any negative values.
-- It is designed to be reusable across multiple models and columns.

{% test not_negative(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} < 0

{% endtest %}

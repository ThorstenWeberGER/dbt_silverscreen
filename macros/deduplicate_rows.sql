-- this macro will return any column_id, column_key etc. by eliminating duplicates based on a given single or combination of columns

/* usage
{{ deduplicate_by_row_number(
    schema='raw',
    table='invoices',
    key_column=''
    partition_by_columns=['invoice_id', 'movie_id']
) }}
*/ 

{% macro deduplicate_rows(schema, table, partition_by_columns) %}

with source_data as (
    select * from {{ source( schema, table ) }}
),

return_deduplicated_rows as (
    select
        *,
        row_number() over(partition by {{ partition_by_columns | join(', ') }} order by 1) as row_count
    from source_data
    qualify row_count = 1
)

select * 
from return_deduplicated_rows

{% endmacro %}
-- load source data into table stg_invoices

with load as (
    select *
    from {{source('raw','invoices')}}
)

select 
    trim(movie_id) as movie_id
    , date_trunc(MONTH, invoice_date) as month
    , to_number(REGEXP_SUBSTR(cinema_id, '\\d+')) AS cinema_id -- read out numeric data
    , invoice_sum
    , load_timestamp
from load
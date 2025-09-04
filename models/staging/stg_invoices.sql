-- load source data into table stg_invoices

-- use macro to get deduplicated data
with deduplicated_invoices as (
    {{ deduplicate_rows(
        schema='raw',
        table='invoices',
        partition_by_columns=['invoice_id', 'movie_id']
) }}
)

-- apply cleaning for some columns
, transform as (
    select 
        invoice_id,
        trim(movie_id) as movie_id
        , date_trunc(MONTH, invoice_date) as rental_month
        , to_number(REGEXP_SUBSTR(cinema_id, '\\d+')) AS cinema_id -- read out numeric data
        , invoice_sum
    from deduplicated_invoices
)

-- aggregate daily values on monthly granularity
, aggregate as (
    select
        movie_id,
        cinema_id,
        rental_month,
        sum(invoice_sum) as sum_rental_costs
    from transform
    group by movie_id, cinema_id, rental_month
    order by movie_id, rental_month, cinema_id
)

select
    *
from aggregate
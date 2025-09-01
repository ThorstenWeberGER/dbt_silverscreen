-- read source data, minor cleaning and create table stg_sales_cinema_02

with load as (
    select *
    from {{source('raw', 'sales_cinema_02')}}
)
select 
    date_trunc(MONTH, day) as month,
    trim(movie_id) as movie_id,
    tickets_sold,
    total_revenue,
    cinema_id,
    load_timestamp
from load
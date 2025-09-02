-- read source data, minor cleaning and create table stg_sales_cinema_01

with load as (
    select *
    from {{source('raw', 'sales_cinema_01')}}
)
select 
    date_trunc(MONTH, day) as month, -- should i move this to integration ?? as i also need to sum and group?
    trim(movie_id) as movie_id,
    tickets_sold,
    total_revenue,
    cinema_id,
    load_timestamp
from load
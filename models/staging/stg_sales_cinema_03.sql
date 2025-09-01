-- read source data, minor cleaning and create table stg_sales_cinema_03

with load as (
    select *
    from {{source('raw', 'sales_cinema_03')}}
)
select 
    date_trunc('month', month) as month, -- make sure the day is always truncated on month
    trim(movie_id) as movie_id,
    sum(tickets_sold) as tickets_sold,
    sum(total_revenue) as total_revenue,
    cinema_id
from load
where lower(product_type) = 'ticket'
group by
    date_trunc('month', month), movie_id, cinema_id
order by 
    movie_id,
    month
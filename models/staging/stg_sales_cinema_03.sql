-- read source data, minor cleaning and create table stg_sales_cinema_03

select 
    date_trunc('month', month) as sales_month, -- make sure the data is always truncated on month. prevent possible future errors if data is delivered on daily granularity like cinema 1 or cinema 2
    trim(movie_id) as movie_id,
    sum(tickets_sold) as tickets_sold,
    sum(total_revenue) as total_revenue,
    cinema_id
from {{source('raw', 'sales_cinema_03')}}
where lower(product_type) = 'ticket'
group by sales_month, movie_id, cinema_id
order by sales_month, movie_id, cinema_id
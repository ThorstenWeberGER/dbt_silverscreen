-- read source data, minor cleaning and aggregate on monthly level and create table stg_sales_cinema_01

select 
    date_trunc(MONTH, day) as sales_month, 
    trim(movie_id) as movie_id,
    sum(tickets_sold) as tickets_sold,
    sum(total_revenue) as total_revenue,
    cinema_id
from {{source('raw', 'sales_cinema_01')}}
group by sales_month, movie_id, cinema_id
order by sales_month, movie_id, cinema_id
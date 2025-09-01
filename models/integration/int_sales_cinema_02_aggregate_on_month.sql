-- aggregate sales on monthly level so all theatre reports are of same granularity

with load as (
    select *
    from {{ref('stg_sales_cinema_02')}}
)

select
    month,
    movie_id,
    cinema_id,
    sum(tickets_sold) as sum_tickets_sold,
    sum(total_revenue) as sum_total_revenue
from load
group by 
    month, movie_id, cinema_id
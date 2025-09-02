-- aggregate all sales and costs data of our movies with monthly aggregation level

with load_costs as (
    select * 
    from {{ref('int_movie_rental_costs')}}
),

load_sales as (
    select *
    from {{ref('int_movie_sales')}}
)

select
    coalesce(movie_title, 'unknown') as movie_title,
    coalesce(movie_genre, 'unknown') as movie_genre,
    coalesce(movie_studio, 'unknown') as movie_studio,
    movie_budget_category,
    coalesce(movie_director, 'unknown') as movie_director,
    coalesce(movie_pg_rating, 'unknown') as movie_pg_rating,
    movie_length_min,           -- there are missing values because of unknown data
    movie_rental_costs,
    c.month,
    c.cinema_id,
    sum_tickets_sold,
    sum_total_revenue

from load_costs as c
    left join load_sales as s
    on c.movie_id = s.movie_id
        and c.month = s.month
        and c.cinema_id = s.cinema_id

order by 
    cinema_id,
    movie_title,
    month
-- aggregate all sales and costs data of our movies with monthly aggregation level

with load_costs as (
    select * 
    from {{ref('int_movie_rental_costs')}}
),

load_sales as (
    select *
    from {{ref('int_movie_sales')}}
)

, merge_data as (
    select
        m.movie_id,
        m.movie_title,
        m.movie_genre,
        m.movie_director,
        m.movie_studio,
        m.movie_pg_rating,
        m.movie_length_min,    
        m.movie_budget,
        m.movie_budget_category,
        m.rental_month,
        m.cinema_id,
        m.movie_rental_costs,
        s.movie_id as movie_id_2,
        s.sales_month,
        row_number() over(partition by s.movie_id, s.cinema_id order by s.sales_month) as screening_cnt_in_months,
        s.cinema_id as cinema_id_2,
        s.tickets_sold,
        s.total_revenue

    from load_costs as m
        full join load_sales as s
        on m.movie_id = s.movie_id
            and m.rental_month = s.sales_month
            and m.cinema_id = s.cinema_id

    order by 
        s.cinema_id,
        s.movie_id,
        s.sales_month
)

-- for all sales information for movies which we do not have in the invoices table information gets imputed by previous month
, impute_data as (
    select 
        movie_id_2 as movie_id,
        coalesce(movie_title, lag(movie_title) over(partition by cinema_id_2, movie_id_2 order by cinema_id_2, movie_id_2, sales_month)) as movie_title,
        coalesce(movie_genre, lag(movie_genre) over(partition by cinema_id_2, movie_id_2 order by cinema_id_2, movie_id_2, sales_month)) as movie_genre,
        coalesce(movie_director, lag(movie_director) over(partition by cinema_id_2, movie_id_2 order by cinema_id_2, movie_id_2, sales_month)) as movie_director,
        coalesce(movie_studio, lag(movie_studio) over(partition by cinema_id_2, movie_id_2 order by cinema_id_2, movie_id_2, sales_month)) as movie_studio,
        coalesce(movie_pg_rating, lag(movie_pg_rating) over(partition by cinema_id_2, movie_id_2 order by cinema_id_2, movie_id_2, sales_month)) as movie_pg_rating,
        coalesce(movie_length_min, lag(movie_length_min) over(partition by cinema_id_2, movie_id_2 order by cinema_id_2, movie_id_2, sales_month)) as movie_length_min,
        coalesce(movie_budget_category, lag(movie_budget_category) over(partition by cinema_id_2, movie_id_2 order by cinema_id_2, movie_id_2, sales_month)) as movie_budget_category,
        cinema_id_2 as cinema_id,
        sales_month,
        screening_cnt_in_months,
        max(screening_cnt_in_months) over(partition by movie_id_2, cinema_id_2 order by sales_month) as screening_duration_in_months,
        tickets_sold,
        total_revenue,
        div0null(
            coalesce(movie_rental_costs, lag(movie_rental_costs) over(partition by cinema_id_2, movie_id_2 order by cinema_id_2, movie_id_2, sales_month)),
            {{ var('rental_costs_corr_factor') }}
        ) as movie_rental_costs,

    from merge_data
    order by 
        cinema_id_2,
        movie_id_2,
        sales_month
)

select *
from impute_data
order by movie_length_min DESC
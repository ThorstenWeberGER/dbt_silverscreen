-- adding true KPIs about movie performance

with aggregate as (
    select 
        movie_id,
        movie_title,
        movie_genre,
        movie_director,
        movie_studio,
        movie_pg_rating,
        movie_length_min,
        movie_budget_category,
        cinema_id,
        min(sales_month) as first_month_on_screen,
        max(screening_duration_in_months) as screening_duration_in_months,
        sum(tickets_sold) as tickets_sold,
        sum(total_revenue) as revenue,
        sum(movie_rental_costs) as movie_rental_costs,
        sum(total_revenue) - sum(movie_rental_costs) as total_brutto_profit,
    from {{ ref('mrt_movies_performance')}}
    group by 
        movie_id, 
        cinema_id, 
        movie_title,
        movie_genre,
        movie_director,
        movie_studio,
        movie_pg_rating,
        movie_length_min,
        movie_budget_category
        
)

, calculate_performance_kpis as (
    select
        movie_id,
        movie_title,
        movie_genre,
        movie_director,
        movie_studio,
        movie_pg_rating,
        movie_length_min,
        movie_budget_category,
        cinema_id,
        first_month_on_screen,
        screening_duration_in_months,
        tickets_sold,
        round(div0null(tickets_sold, screening_duration_in_months), 0) as tickets_sold_per_month,
        revenue,
        round(div0null(revenue, screening_duration_in_months), 0) as revenue_per_month,
        movie_rental_costs,
        round(div0null(movie_rental_costs, screening_duration_in_months),0) as movie_rental_costs_per_month,
        total_brutto_profit,
        round( div0null(  total_brutto_profit, screening_duration_in_months), 0) as avg_brutto_profit,
        round( div0null( total_brutto_profit,  movie_rental_costs ) * 100 , 2) as brutto_profit_percent
    from aggregate
)

select *
from calculate_performance_kpis
order by movie_id, cinema_id, brutto_profit_percent desc

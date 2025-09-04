-- generate one table which holds all rental costs and descriptive data of shown movies

-- preload data
with
    load_movies as (
        select * 
        from {{ ref("stg_movie_catalog") }})
    , load_invoices as (
        select * 
        from {{ ref("stg_invoices") }})
-- join data
    , join_data as (
        select
            m.movie_id,
            m.movie_title,
            m.movie_genre,
            m.movie_director,
            m.movie_studio,
            m.movie_pg_rating,
            m.movie_length_min,    
            m.movie_budget,
            i.rental_month,
            i.cinema_id,
            i.sum_rental_costs as movie_rental_costs
        from  load_invoices as i -- I prioritize having rental_costs from invoices, even if there are no qualitative movie information
        left join load_movies as m 
            on m.movie_id = i.movie_id
    ) 
-- add movie_budget_category
select 
    *,
    case
        when movie_budget is null then null 
        when movie_budget <= 100000000 then 'small'
        when movie_budget <= 200000000 then 'medium'
        else 'large'
    end as movie_budget_category
from join_data
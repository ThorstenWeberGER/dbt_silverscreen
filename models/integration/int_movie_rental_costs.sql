-- generate one table which holds all rental costs and descriptive data of shown movies
-- preload data
with
    load_movies as (select * from {{ ref("stg_movie_catalog") }}),
    load_invoices as (select * from {{ ref("stg_invoices") }})

-- join data
select
    m.movie_id,
    m.movie_title,
    m.movie_genre,
    m.movie_studio,
    m.movie_studio_country,
    m.movie_budget,
    case
        when m.movie_budget <= 100000000
        then 'small'
        when m.movie_budget <= 200000000
        then 'medium'
        else 'large'
    end as movie_budget_category,
    m.movie_director,
    m.movie_pg_rating,
    m.movie_length_min,
    i.month,
    i.cinema_id,
    i.invoice_sum as movie_rental_costs
from load_movies as m
    left join load_invoices as i 
    on m.movie_id = i.movie_id

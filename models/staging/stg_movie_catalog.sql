with load as (
    select *
    from {{source('raw', 'movies')}}
)

select 
    trim(movie_id) as movie_id,
    initcap(trim(movie_title)) as movie_title,
    movie_release_date,
    initcap(trim(genre)) as movie_genre,
    upper(trim(country)) as movie_studio_country,
    initcap(trim(studio)) as movie_studio,
    pg_rating as movie_pg_rating,
    movie_length_min,
    budget as movie_budget,
    initcap(trim(director)) as movie_director
from load
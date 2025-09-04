-- load relevant columns, clean, standardize format, secure data quality, fill missing values, create table stg_movie_catalog.

select 
    trim(movie_id) as movie_id,
    coalesce(initcap(trim(movie_title)), 'unknown') as movie_title,
    coalesce(initcap(trim(genre)), 'unknown') as movie_genre,
    coalesce(initcap(trim(director)), 'unknown') as movie_director,
    coalesce(initcap(trim(studio)), 'unknown') as movie_studio,
    coalesce(trim(pg_rating), 'unknown') as movie_pg_rating,
    movie_length_min,
    budget as movie_budget,
from {{source('raw', 'movies')}}
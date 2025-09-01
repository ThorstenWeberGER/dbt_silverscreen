-- unite sales figures from three models into one sales table

with load_sales_cinema_01 as (
    select *
    from {{ref('int_sales_cinema_01_aggregate_on_month')}}
),
load_sales_cinema_02 as (
    select *
    from {{ref('int_sales_cinema_02_aggregate_on_month')}}
),
load_sales_cinema_03 as (
    select
        month,
        movie_id,
        cinema_id,
        tickets_sold as sum_tickets_sold,
        total_revenue as sum_total_revenue
    from {{ref('stg_sales_cinema_03')}}
)

select * from load_sales_cinema_01
union all
select * from load_sales_cinema_02
union all
select * from load_sales_cinema_03

order by
    month,
    cinema_id,
    movie_id
-- this script compares the KPIs 'sold_tickets', 'revenue', 'rental_costs' 
-- between the raw_data and final_mrt. it helps to spot differences ensuring data
-- quality
{{ config(store_failures=true) }}

with
    -- prepare individual raw data sources, read out relevant kpis for comparison
    raw_sales_cinema_01 as (
        select
            sum(tickets_sold) as raw_sum_tickets_sold,
            sum(total_revenue) as raw_sum_total_revenue
        from {{ source("raw", "sales_cinema_01") }}
    ),
    raw_sales_cinema_02 as (
        select
            sum(tickets_sold) as raw_sum_tickets_sold,
            sum(total_revenue) as raw_sum_total_revenue
        from {{ source("raw", "sales_cinema_02") }}
    ),
    raw_sales_cinema_03 as (
        select
            sum(tickets_sold) as raw_sum_tickets_sold,
            sum(total_revenue) as raw_sum_total_revenue
        from {{ source("raw", "sales_cinema_03") }}
        where product_type = 'ticket'
    ),
    raw_rental_costs as (
        select sum(invoice_sum) as raw_sum_rental_costs
        from {{ source("raw", "invoices") }}
    ),
    -- read out kpis from mart layer, which are all based on kpis from raw data sources
    mrt_sales_costs_all_cinemas as (
        select
            sum(movie_rental_costs) as mrt_sum_rental_costs,
            sum(tickets_sold) as mrt_sum_tickets_sold,
            sum(total_revenue) as mrt_sum_total_rewvenue
        from {{ ref("mrt_movies_performance") }}
    ),
    -- bring sales from different movies in one result table
    raw_sales_all_cinemas as (
        select *
        from raw_sales_cinema_01
        union
        select *
        from raw_sales_cinema_02
        union
        select *
        from raw_sales_cinema_03
    ),
    -- aggregate sales from different movies theatres as required for later comparison
    raw_sales_all_cinemas_aggregated as (
        select
            sum(raw_sum_tickets_sold) as raw_sum_tickets_sold,
            sum(raw_sum_total_revenue) as raw_sum_total_revenue
        from raw_sales_all_cinemas
    ),
    -- compare kpis
    compare_kpis as (
        select
            'tickets_sold' as kpi,
            raw_sum_tickets_sold as raw,
            mrt_sum_tickets_sold as mrt,
            raw_sum_tickets_sold = mrt_sum_tickets_sold as is_ok
        from
            raw_sales_all_cinemas_aggregated,
            raw_rental_costs,
            mrt_sales_costs_all_cinemas
        union
        select
            'total_revenue' as kpi,
            raw_sum_total_revenue as raw,
            mrt_sum_total_rewvenue as mrt,
            raw_sum_total_revenue = mrt_sum_total_rewvenue as is_ok
        from
            raw_sales_all_cinemas_aggregated,
            raw_rental_costs,
            mrt_sales_costs_all_cinemas
        union
        select
            'total_rental_costs' as kpi,
            raw_sum_rental_costs as raw,
            mrt_sum_rental_costs as mrt,
            raw_sum_rental_costs = mrt_sum_rental_costs as is_ok
        from
            raw_sales_all_cinemas_aggregated,
            raw_rental_costs,
            mrt_sales_costs_all_cinemas
    )

-- show kpis which do not match
select *
from compare_kpis
where is_ok = false

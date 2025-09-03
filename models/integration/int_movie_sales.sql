-- unite sales figures from three cinemas into one sales table

select * 
from {{ref('stg_sales_cinema_01')}}
union all
select * 
from {{ref('stg_sales_cinema_02')}}
union all
select *
from {{ref('stg_sales_cinema_03')}}
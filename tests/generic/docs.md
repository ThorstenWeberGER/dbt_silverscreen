-- tests / generic / docs.model

-- The doc block below is what adds the description to your documentation.
{%- docs not_negative -%}
This generic test checks that the values of given columns are zero or positive numbers.
{%- enddocs -%}

-- The doc block below is what adds the description to your documentation.
{%- docs not_earlier_than_threshold_date -%}
This generic test checks if a date column has values earlier than threshold
set threshold in dbt_project.yml. The var 'threshold_date' is defined in dbt_project.yml.
{%- enddocs -%}

-- The doc block below is what adds the description to your documentation.
{%- docs compare_kpi_raw_mrt -%}
This test compares the KPIs 'sold_tickets', 'revenue', 'rental_costs' 
between the raw_data and final_mrt. it helps to spot differences ensuring data
quality of the resulting marts models.
{%- enddocs -%}
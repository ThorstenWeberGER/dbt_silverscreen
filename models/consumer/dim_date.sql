-- required for semantic layer only

-- Step 1: Define the start and end dates as Jinja variables right here in the file.
{% set start_date = "2024-03-01" %}

-- Use dbt's built-in 'run_started_at' variable to get the current date dynamically.
-- This avoids the problematic dbt_date.today() macro and its timezone dependency.
{% set end_date = run_started_at. strftime('%Y-%m-%d') %}

-- Step 2: Pass these variables directly into the macro.
{{ config(materialized='table') }}

{{ dbt_date.get_date_dimension(start_date, end_date) }}
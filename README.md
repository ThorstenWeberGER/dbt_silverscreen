# ETL - Pipeline with Snowflake and dbt

## Project Purpose

This project is part of my curriculum at Masterschool. It integrates using Snowflake as the datawarehouse and data built tool (dbt) for creation and automation of the data pipeline.

## Management Summary

## Lessons Learned and Skills Built

## The Process

### Setup/ Tooling

This is not spectactular but technical setup is of course required
* Created new Snowflake environment
    * database and different schemas for development and production 
    * data storage for staging of raw data
    * data type for import of the csv files
* Created new dbt account in dbt cloud
    * connection to Snowflake
    * setting up two environments for dev and prod
    * integration with Github for version control
    * added test packages (dbt-utils, dbt-expectations)

This time I decided to work cloud native instead of setting up VS Code as IDE and syncing via Github. I want to make use of the great data analysis features which are built in Snowflake for the EDA part of this project.

### Exploring the data

Note: Within the initial EDA I already identified some requirements for later transformation


### ETL-Pipeline: Ingestion

This is a great feature of snowflake to directly connect to cloud storage providers and read in source files in various formats like csv or parquet. As this is the typical real-life scenario and we were given 5 CSV files I chose to simulate the cloud storage ingestion process, created a snowflake managed cloud storage and uploaded the csv files.

**Steps**:
* Identify columns required 
* DDL scripts to create schema `raw` as well as five `tables`
* Normalize and standardise column names and table names
* Use COPY INTO scripts to upload selected columns into tables
* in dbt created schema.yml to define sources for later use

CODE EXAMPLE
`this is code
`

`LINK COMPLETE CODE`

**Tests** applied: Do tests as early as possible to assure data quality. Thus we do right away at source level.
`is that possible???`


### ETL-Pipeline: Staging 

**The phase covers** basic data type conversions and cleaning By convention there are no big transformations in this step. 

Applied **transformations in Data models**:
* stg_sales_cinema_01:
    * day: conversion into monthly data 
* stg_sales_cinema_02:
    * day: conversion into monthly data 
* stg_sales_cinema_03:
    * no transformation required at this stage
    * day: conversion into monthly data just to make sure we all have truncated, monthly data for later use
* stg_movies
    * basically only applied trim and initcap or upper for string columns for data quality
* stg_invoices
    * also applied trim and initcap for string columns
    * applied date_trunc(MONTH, ..) for invoice_date columns to prepare for monthly aggregation
    * read_out the movie_budget in USD out of string (    , to_number(REGEXP_SUBSTR(cinema_id, '\\d+')) AS cinema_id)
Applied **tests**: EXAMPLERRARY DESCRIPTION - FOR MORE SEE DOCUMENTATION

### ETL-Pipeline: Integration

### ETL-Pipeline: Consumer

## Insights

## Recommendations

## Personal takeaways

* **Snowflakes datatype definition** feature is great to immediately address data quality issues
    * ENCODING = 'UTF8': Specifies the character encoding of the file. UTF-8 is the standard and most common encoding.
    * ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE: This setting ensures that the load operation will fail if a row does not have the same number of columns as the others. This is a great way to catch data quality issues early.
    * TRIM_SPACE = TRUE: Automatically removes leading and trailing white spaces from each string column, which can prevent unexpected errors and multiplication of categorical values only by whitespace.
    * EMPTY_FIELD_AS_NULL = TRUE: This option treats any empty, unquoted field as NULL, which is often the desired behavior for missing data.
* **xxxx**
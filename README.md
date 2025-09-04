# ETL - Pipeline with dbt and Snowflake

![silverscreen_logo](images/silverscreen_logo_medium.png)

## Project Purpose

This project is part of my curriculum at Masterschool. Main goal is to utilize the data built tool (dbt) for setting up, managing, testing and orchestration a data pipeline for a fictive movie theatre company. 

The pipeline shall enable a dataset for assessing the performance of movies shown across all locations. The data is stored in a Snowflake datawarehouse.

## Management Summary

## Lessons Learned and Skills Built
* designing a Medaillon data layer model with multiple data models for transformation, cleaning and aggregation of data
* setting up the right data tests at the right spot to safe guard data quality (used custom tests, generic tests, test packages like dbt_utils or dbt_expectations)
* defining configuration files in YML
* mastered Jinja language for added flexibility in SQL scripts and DRY code
* documentation which is to the spot, crisp and not overwhelming

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

Within the initial exploratory data analysis (EDA) first anomalies showed already. Overall following anomalies exist. Many of them could be healed by different methods.

* missing values in various qualitative but also in numeric fields
* records for which no matching data can be found in others tables
* fully and partly duplicated data
* significantly high costs which cannot be compensated by revenue and create high losses

The methods for healing nearly of all them are described later in the paragraphs regarding the models and transformation and cleaning applied. Note: high costs cannot be healed. The reason needs to be investigated further.


### ETL-Pipeline: Ingestion in Snowflake

**The phase covers** a great feature of snowflake to directly connect to cloud storage providers and read in source files in various formats like csv or parquet. As this is the typical real-life scenario and we were given 5 CSV files I chose to simulate the cloud storage ingestion process, created a snowflake managed cloud storage and uploaded the csv files.

**Steps**:
* Identify columns required 
* DDL scripts to create schema `raw` as well as five `tables`
* Normalize and standardise column names and table names
* Use COPY INTO scripts to upload selected columns into tables

[code.sql](models/ingestation/load_raw_data.sql)

**Tests** applied: 
* not_null
* custom not_negative for tickets_sold, revenue and costs data
* custom test for accessing if date columns hold recent values
* unique test for movie_ids in movies source
* check for primary keys and foreign keys consistency across sources

### ETL-Pipeline: Staging 

**The phase covers** basic data type conversions and cleaning By convention there are no big transformations in this step. 

Applied **transformations and cleaning**:
* `stg_sales_cinema_01 and stg_sales_cinema_02`:
    * aggregate data on a monthly granularity
    * added feature to identify from which cinema the sales data comes from
* `stg_sales_cinema_03`:
    * data is already on monthly granularity. To safeguard the pipline still aggregation is applied if cinema three delivers data on daily level in the future
* `stg_movies`:
    * trim and initcap or upper for string columns for data quality
    * imputation of missing values with 'unknown' for string columns
    * identified missing values in movies_lenght not imputed, remains NULL in that case
* `stg_invoices`:
    * eliminated redundant invoice information
    * also applied trim and initcap for string columns
    * applied date_trunc(MONTH, ..) for invoice_date columns to prepare for monthly aggregation
    * extracted numeric values out of string columns (i.e. movie_budget and cinema_id)

**Tests** applied: Mainly not_null, a custom not_negative test and a custom test for accessing if date columns hold recent values. Additionally applied accepted_values for checken on correct cinema_ids.

### ETL-Pipeline: Integration

* `int_movie_rental_costs`: Unioned all sales data from different movie theatres into one table.
* `int_movie_sales`: Joined invoice data with qualitative movie information from stg_movies

**Tests** applied: No tests applied due to data is not changed.

### ETL-Pipeline: Consumer

Two models exist for the reason that both differ in granularity. One model has time-series data. The other model aggregates data on movie and cinema presenting KPIs to assess and compare the movies performance like avg_tickets_sold, total_profit and more.

* `mrt_movies_performance`: time-series data on monthly level
    * used full outer join for models int_movie_rental_costs with int_movie_sales
    * imputed missing values in int_movie_rental_costs with information from previous month information
    * added features screening_months_count and screening_duration_in_months required in later models
* `mrt_movies_performance_incl_kpis`: aggregated on movie and cinema
    * added features *first_month_on_screen*, calculated *total_brutto_profit* (i.e. revenue - rental_costs before infrastructure and personal)
    * added additional kpis for better movie performance comparison:
        * tickets_sold_per_month
        * movie_rental_costs_per_month
        * avg_brutto_profit
        * brutto_profit_percent (which is kind or a ROI)

**Tests** applied: Mainly not_null, not_negative or accepted_values on all fields to safeguard data quality at mart level.

## Insights

`t.b.d.`

## Recommendations

**Business aspects**
* `t.b.d.`

**Data quality**
* standardise reporting across cinemans (fields, aggregation level)
* enforce filling in fields about movies when entering the data
* include a timestamp per source dataset showing the reporting time and being used for data freshness checks
* implement a test which compares the key KPIs like tickets sold, revenue and costs across the layers of model from raw till mart to make sure, the data shown is matching the data given

Finally, we suggest to `investigate duplicates and missing invoices`.  

## Personal takeaways

* **Snowflakes datatype definition** feature is great to immediately address data quality issues
    * ENCODING = 'UTF8': Specifies the character encoding of the file. UTF-8 is the standard and most common encoding.
    * ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE: This setting ensures that the load operation will fail if a row does not have the same number of columns as the others. This is a great way to catch data quality issues early.
    * TRIM_SPACE = TRUE: Automatically removes leading and trailing white spaces from each string column, which can prevent unexpected errors and multiplication of categorical values only by whitespace.
    * EMPTY_FIELD_AS_NULL = TRUE: This option treats any empty, unquoted field as NULL, which is often the desired behavior for missing data.
* **xxxx**
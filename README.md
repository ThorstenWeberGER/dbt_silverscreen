# Analytics Engineering: ETL - Pipeline with dbt, GitHub and Snowflake

![silverscreen_logo](images/silverscreen_logo_medium.png)

## Project Purpose

This curriculum project was a deep dive into building a robust, **production-ready Analytics Engineering pipeline**. It wasn't just about moving data-it was about transforming a complex, messy dataset into a single source of truth to answer a critical business question: Why is our movie theatre company losing money?

Using **data built tool (dbt) and Snowflake**, I designed, built, and tested a full ETL pipeline, delivering critical KPIs to assess the financial performance of movies across all locations. The key insights were finally visualized in **Tableau dashboard**.

## 🛠️ Core Technologies & Stack

This project demonstrates expertise across the full modern data stack, emphasizing scalability and code quality:

* **Data Warehouse**: Snowflake ❄️ (Cloud-native environment, enhanced SQL/Python features, schema management, COPY INTO ingestion).

* **Transformation & Orchestration**: dbt (data built tool), a cloud-native development environment for model definition, testing, documentation, and orchestration.

* **Version Control**: Git/GitHub.

* **Visualization**: Tableau (Used for final business performance dashboards).

* **Code Languages**: SQL, Jinja, and YAML.

## ⚙️ Key Analytics Engineering Accomplishments

This project highlights my ability to move beyond simple queries to build resilient and well-governed data assets:

* **Data Modeling**: Designed a Medallion Data Model (Raw/Staging/Integration/Consumer) with 10+ dbt models to manage complexity and ensure clear data lineage.

    ![Show the model lineage.](images/models_lineage.png)

* **Data Quality & Testing**: Implemented comprehensive testing early and often using a mix of built-in dbt tests, custom self-made tests, and packages like dbt-utils and dbt-expectations. This included:

    * Uniqueness and Not-Null checks at the Source and Mart levels.

    * Custom tests like not_negative for financial metrics (tickets_sold, revenue, costs).

* **Code DRYness & Reusability**: Leveraged Jinja and dbt Macros (as simple as Python functions!) to significantly reduce repetitive SQL code and manage logic efficiently.

* **Cloud Ingestion**: Simulated a real-world scenario by using Snowflake's COPY INTO feature to ingest raw data from a managed cloud storage, standardizing schemas and column names.

* **Documentation & Governance**: Utilized dbt’s functionality to generate detailed, structured, and readable project documentation and data lineage automatically, simultaneously with development.

## 📉 Business Impact & Actionable Insights

The resulting data mart (mrt_movies_performance_incl_kpis) immediately exposed a crucial business problem:

| Insight | Visualization |
| --- | ---: |
| **The Financial Picture**: The company is operating at an overall loss over the analysed 12 months, driven primarily by two major locations. | <a href="images/analysis_1.png"><img src="images/analysis_1.png" alt="overall_profitability" width="300"/></a> |
| **Profitability Paradox**: The data revealed that unprofitable movies were screened for up to 18 months, while profitable movies were quickly rotated out after only a few months. | <a href="images/analysis_2.png"><img src="images/analysis_2.png" alt="genre_profitability" width="300"/></a> |
| **Targeted Loss**: Identified specific movie genres that are highly unprofitable and account for a significant percentage of total losses. | <a href="images/analysis_3.png"><img src="images/analysis_3.png" alt="movie_profitability_screentime" width="300"/></a> |

**👉 Recommendations**: Focus on profitable movie genres, investigate reasons for showing money-losing films, and extend screening durations for profitable titles to capitalize on market demand.

Link to [Tableau Dashboard](https://public.tableau.com/app/profile/thorsten.weber/viz/movie_performance_FY24-25)


## Comprehensive Technical project description

The following passages are for the technically interested readers. You will find information about the data tools involved, steps and methods for data cleaning and transformation and testing of the models.

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

This time I decided to work cloud native instead of dbt core local. I want to make use of the great data analysis features which are built in Snowflake for the EDA part of this project.

### Exploring the data

Within the initial exploratory data analysis (EDA) first anomalies showed already. Overall following anomalies exist. Many of them could be healed by different methods.

* missing values in various qualitative but also in numeric fields
* records for which no matching data can be found in others tables
* fully and partly duplicated data in invoices
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

### Transformation process using dbt

Use this lineage as a reference to better understand how the detailed models work together.

![Show the model lineage.](images/models_lineage.png)

#### ETL-Pipeline: Staging 

The phase covers basic data type conversions and cleaning By convention there are no big transformations in this step. 

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

#### ETL-Pipeline: Integration

* `int_movie_rental_costs`: Unioned all sales data from different movie theatres into one table.
* `int_movie_sales`: Joined invoice data with qualitative movie information from stg_movies

**Tests** applied: No tests applied due to data is not changed.

#### ETL-Pipeline: Consumer

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
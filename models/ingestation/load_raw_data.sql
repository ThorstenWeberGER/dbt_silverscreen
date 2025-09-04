-------------------------------------------------------------------------------
-- silverscreen project. this script will ingest data from defined staging area 
-- into newly created tables. formerly existing data will be deleted.
-- datawarehouse: snowflake
-------------------------------------------------------------------------------

-- configure database
use database silverscreen;
use schema public;
use role accountadmin;
use warehouse compute_wh;


-- create datatype for csv import
CREATE OR REPLACE FILE FORMAT silverscreen.public.CSV_FILE_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  ENCODING = 'UTF8'
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ;

-- import raw files from staging into schema rawSILVERSCREEN.RAW.SALES_CINEMA_01
create or replace schema raw;
use schema raw;

create or replace table invoices ( 
    movie_id varchar, 
    invoice_id varchar,
    invoice_date date,
    cinema_id varchar, 
    movie_studio varchar, 
    movie_release_date date, 
    invoice_sum int,
    load_timestamp timestamp
);

COPY INTO invoices 
FROM (
  SELECT
    $1::VARCHAR, -- movie_id
    $2::varchar, -- invoice_id
    $3::date,    -- invoice_month
    $4::varchar, -- cinema_id
    $5::varchar, -- movie_studio
    $6::date,    -- movie_release_date
    $8::int,     -- invoice_sum
    current_timestamp() 
  FROM @silverscreen.public.datasources
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FILE_FORMAT')
FILES = ('invoices.csv')
;


create or replace table movies (
    movie_id varchar,
    movie_title varchar,
    movie_release_date date,
    genre varchar,
    country varchar,
    studio varchar,
    budget varchar,
    director varchar,
    pg_rating varchar,
    movie_length_min int,
    load_timestamp timestamp
);

COPY INTO movies 
FROM (
  SELECT
    $1, --movie_id
    $2, --movie_title
    $3::date,    --release_date
    $4, --genre
    $5, --country
    $6, --studio
    $7, --budget -- required transformation into number
    $8, --director
    $9, --rating
    $10::int,    --minutes
    current_timestamp() 
  FROM @silverscreen.public.datasources
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FILE_FORMAT')
FILES = ('movie_catalogue.csv')
;


create or replace table sales_cinema_01 (
    day date,
    movie_id varchar,
    tickets_sold int,
    total_revenue int,
    cinema_id int,
    load_timestamp timestamp
);

copy into sales_cinema_01
from
    (select
        $1::date,
        $3 as movie_id,
        $4 as tickets_sold,
        $6 as total_revenue,
        1 as cinema_id,
        current_timestamp()
    from @silverscreen.public.datasources
    )
file_format = (format_name = 'CSV_FILE_FORMAT')
FILES = ('nj_001.csv')
;




create or replace table sales_cinema_02 (
    day date,
    movie_id varchar,
    tickets_sold int,
    total_revenue int,
    cinema_id int,
    load_timestamp timestamp
);

copy into sales_cinema_02
from
    (select
        $1::date,
        $2 as movie_id,
        $3 as tickets_sold,
        $5 as total_revenue,
        2 as cinema_id,
        current_timestamp()
    from @silverscreen.public.datasources
    )
file_format = (format_name = 'CSV_FILE_FORMAT')
FILES = ('nj_002.csv')
;

create or replace table sales_cinema_03 (
    month date,
    movie_id varchar,
    product_type varchar,
    tickets_sold int,
    total_revenue int,
    cinema_id int,
    load_timestamp timestamp
);

copy into sales_cinema_03
fromSILVERSCREEN.RAW.MOVIESSILVERSCREEN.RAW.MOVIES
    (select
        $1::date,
        $4 as movie_id,
        $3 as product_type,
        $5 as tickets_sold,
        $7 as total_revenue,
        3 as cinema_id,
        current_timestamp()
    from @silverscreen.public.datasources
    )
file_format = (format_name = 'CSV_FILE_FORMAT')
FILES = ('nj_003.csv')
;

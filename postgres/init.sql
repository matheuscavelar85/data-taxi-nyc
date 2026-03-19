CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.taxi_rides_staging (
    source_file           TEXT,
    vendor_id             TEXT,
    pickup_datetime       TEXT,
    dropoff_datetime      TEXT,
    passenger_count       INTEGER,
    trip_distance         FLOAT,
    pickup_longitude      FLOAT,
    pickup_latitude       FLOAT,
    rate_code_id          TEXT,
    store_and_fwd_flag    TEXT,
    dropoff_longitude     FLOAT,
    dropoff_latitude      FLOAT,
    payment_type          TEXT,
    fare_amount           FLOAT,
    extra                 FLOAT,
    mta_tax               FLOAT,
    tip_amount            FLOAT,
    tolls_amount          FLOAT,
    improvement_surcharge FLOAT,
    total_amount          FLOAT,
    ingestion_timestamp   TIMESTAMPTZ DEFAULT NOW()
);

CREATE SCHEMA IF NOT EXISTS dbt_dev;
CREATE SCHEMA IF NOT EXISTS dbt_dev_raw;
CREATE SCHEMA IF NOT EXISTS dbt_dev_silver;
CREATE SCHEMA IF NOT EXISTS dbt_dev_gold;

GRANT USAGE ON SCHEMA raw TO dbt_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO dbt_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO dbt_user;

GRANT USAGE ON SCHEMA dbt_dev TO dbt_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dbt_dev TO dbt_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dbt_dev TO dbt_user;

GRANT USAGE ON SCHEMA dbt_dev_raw TO dbt_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dbt_dev_raw TO dbt_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dbt_dev_raw TO dbt_user;

GRANT USAGE ON SCHEMA dbt_dev_silver TO dbt_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dbt_dev_silver TO dbt_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dbt_dev_silver TO dbt_user;

GRANT USAGE ON SCHEMA dbt_dev_gold TO dbt_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dbt_dev_gold TO dbt_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dbt_dev_gold TO dbt_user;

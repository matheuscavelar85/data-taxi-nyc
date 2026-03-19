{{ config(
    materialized='view',
    schema='raw'
) }}

select
    pickup_datetime,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    passenger_count,
    fare_amount,
    ingestion_timestamp,
    current_timestamp as dbt_loaded_at
from raw.taxi_rides_staging


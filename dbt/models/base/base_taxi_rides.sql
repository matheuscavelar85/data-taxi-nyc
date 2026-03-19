{{ config(
    materialized='view',
    schema='raw'
) }}

with source as (
    select
        pickup_datetime,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        passenger_count,
        fare_amount,
        ingestion_timestamp,
        current_timestamp as dbt_loaded_at,

        row_number() over (
            partition by
                pickup_datetime,
                pickup_longitude,
                pickup_latitude,
                dropoff_longitude,
                dropoff_latitude,
                passenger_count,
                fare_amount
            order by ingestion_timestamp desc
        ) as rn

    from raw.taxi_rides_staging
)

select
    pickup_datetime,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    passenger_count,
    fare_amount,
    ingestion_timestamp,
    dbt_loaded_at

from source
where rn = 1
{{ config(
    materialized='table',
    schema='gold'
) }}

select
    ride_id,
    pickup_datetime,
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    borough_approximation,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    passenger_count_clean as passenger_count,
    fare_amount_clean as fare_amount,
    trip_distance_km,
    trip_duration_minutes,
    round(cast(passenger_count_clean / nullif(trip_duration_minutes, 0) * 60 as numeric), 2) as implied_speed_kmh,
    round(cast(fare_amount_clean / nullif(trip_distance_km, 0) as numeric), 2) as fare_per_km,
    ingestion_timestamp,
    dbt_processed_at,
    now() as dbt_gold_at
    
from {{ ref('stg_taxi_rides') }}

order by pickup_datetime desc

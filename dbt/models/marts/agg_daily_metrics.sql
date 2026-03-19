{{ config(
    materialized='table',
    schema='gold'
) }}

select
    pickup_date,
    pickup_day_of_week,
    count(distinct ride_id) as total_rides,
    count(distinct passenger_count) as total_passengers,
    round(cast(sum(fare_amount) as numeric), 2) as total_fare_revenue,
    round(cast(avg(fare_amount) as numeric), 2) as avg_fare_amount,
    round(cast(min(fare_amount) as numeric), 2) as min_fare_amount,
    round(cast(max(fare_amount) as numeric), 2) as max_fare_amount,
    round(cast(avg(trip_distance_km) as numeric), 2) as avg_trip_distance_km,
    round(cast(sum(trip_distance_km) as numeric), 2) as total_trip_distance_km,
    round(cast(avg(passenger_count) as numeric), 2) as avg_passengers_per_ride,
    round(cast(sum(fare_amount) / nullif(sum(trip_distance_km), 0) as numeric), 2) as avg_fare_per_km,
    now() as dbt_aggregated_at
    
from {{ ref('fct_rides') }}

group by pickup_date, pickup_day_of_week
order by pickup_date desc

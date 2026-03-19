{{ config(
    materialized='view',
    schema='silver'
) }}

with raw_data as (
    select * from {{ ref('base_taxi_rides') }}
),

cleaned as (
    select
        md5(
            coalesce(pickup_datetime, '') || '-' ||
            coalesce(cast(pickup_longitude as varchar), '') || '-' ||
            coalesce(cast(pickup_latitude as varchar), '') || '-' ||
            coalesce(cast(passenger_count as varchar), '')
        ) as ride_id,
        pickup_datetime,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        passenger_count,
        fare_amount,
        ingestion_timestamp,
        
        case 
            when passenger_count <= 0 then null
            else passenger_count 
        end as passenger_count_clean,
        
        case 
            when fare_amount <= 0 then null
            else fare_amount 
        end as fare_amount_clean,
        
        round(
            cast(111.0 * sqrt(
                power(pickup_latitude - dropoff_latitude, 2) +
                power((pickup_longitude - dropoff_longitude) * cos(radians(pickup_latitude)), 2)
            ) as numeric), 
            2
        ) as trip_distance_km,
        
        15 as trip_duration_minutes,
        
        cast(pickup_datetime as timestamp)::date as pickup_date,
        extract(hour from cast(pickup_datetime as timestamp)) as pickup_hour,
        extract(dow from cast(pickup_datetime as timestamp)) as pickup_day_of_week,
        
        case 
            when pickup_longitude between -74.02 and -73.93 and pickup_latitude between 40.70 and 40.82 then 'Manhattan'
            when pickup_longitude between -74.03 and -73.92 and pickup_latitude between 40.58 and 40.70 then 'Brooklyn'
            when pickup_longitude between -73.93 and -73.83 and pickup_latitude between 40.79 and 40.87 then 'Bronx'
            when pickup_longitude between -73.97 and -73.82 and pickup_latitude between 40.65 and 40.80 then 'Queens'
            when pickup_longitude between -74.26 and -74.05 and pickup_latitude between 40.51 and 40.65 then 'Staten Island'
            else 'Unknown'
        end as borough_approximation,
        
        case 
            when pickup_longitude between -74.3 and -73.7
                and pickup_latitude between 40.5 and 40.9
            then 'valid'
            else 'out_of_bounds'
        end as location_validation,
        
        now() as dbt_processed_at
        
    from raw_data
    
    where 
        pickup_datetime is not null
        and pickup_longitude is not null
        and pickup_latitude is not null
        and dropoff_longitude is not null
        and dropoff_latitude is not null
)

select * from cleaned
where location_validation = 'valid'
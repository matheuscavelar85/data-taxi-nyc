{{ config(
    materialized='table',
    schema='gold'
) }}

with pickup_locations as (
    select distinct
        pickup_longitude as longitude,
        pickup_latitude as latitude,
        'pickup' as location_type
    from {{ ref('stg_taxi_rides') }}
    where pickup_longitude is not null and pickup_latitude is not null
),

dropoff_locations as (
    select distinct
        dropoff_longitude as longitude,
        dropoff_latitude as latitude,
        'dropoff' as location_type
    from {{ ref('stg_taxi_rides') }}
    where dropoff_longitude is not null and dropoff_latitude is not null
),

all_locations as (
    select * from pickup_locations
    union all
    select * from dropoff_locations
),

ranked_locations as (
    select
        md5(cast(longitude || '-' || latitude as varchar)) as location_id,
        longitude,
        latitude,
        row_number() over (partition by longitude, latitude order by location_type) as rn
    from all_locations
)

select
    location_id,
    longitude,
    latitude,
    case
        when latitude between 40.7 and 40.8 and longitude between -74.0 and -73.95 then 'Manhattan South'
        when latitude between 40.8 and 40.85 and longitude between -74.0 and -73.95 then 'Manhattan Midtown'
        when latitude between 40.85 and 40.9 and longitude between -74.0 and -73.95 then 'Manhattan North'
        when latitude < 40.7 then 'Brooklyn'
        when latitude > 40.9 then 'Bronx'
        else 'Queens'
    end as borough_approximation,
    now() as dbt_loaded_at
    
from ranked_locations
where rn = 1
order by latitude, longitude

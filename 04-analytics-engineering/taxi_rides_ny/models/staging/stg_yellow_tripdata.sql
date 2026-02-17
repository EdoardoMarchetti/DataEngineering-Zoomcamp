SELECT 
    -- identifiers
    cast(vendorid as integer) as vendor_id,
    cast(ratecodeid as integer) as rate_code_id,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,

    --timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as float) as trip_distance,
    1 as trip_type, -- Yellow trips are always street-hail

    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    0 as ehail_fee, -- Yellow trips do not have ehail fee
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    

FROM {{ source('raw_data', 'yellow_tripdata') }}
WHERE vendorid is not null
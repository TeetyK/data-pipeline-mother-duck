SELECT
hvfhs_license_num,
dispatching_base_num,
originating_base_num,

cast(request_datetime as timestamp) as request_ts,
cast(on_scene_datetime as timestamp) as on_scene_ts,
cast(pickup_datetime as timestamp) as pickup_ts,
cast(dropoff_datetime as timestamp) as dropoff_ts,
cast(pickup_datetime as timestamp) as pickup_date,

cast(PULocationID as int64) as PULocationID,
cast(DOLocationID as int64) as DOLocationID,

cast(trip_miles as float64) as trip_miles,
cast(trip_time as int64) as trip_time,

cast(base_passenger_fare as numeric) as base_passenger_fare,
cast(tolls as numeric) as tolls,
cast(tips as numeric) as tips,
cast(bcf as numeric) as bcfee,
cast(sales_tax as numeric) as sales_tax,
cast(congestion_surcharge as numeric) as congestion_surcharge,
cast(airport_fee as numeric) as airport_fee,
cast(driver_pay as numeric) as driver_pay,
cast(cbd_congestion_fee as numeric) as cbd_congestion_fee,

shared_request_flag = 'Y' as is_shared_request,
shared_match_flag = 'Y' as is_shared_match,
access_a_ride_flag = 'Y' as is_access_a_ride,
wav_request_flag = 'Y' as is_wav_request,
wav_match_flag = 'Y' as is_wav_match,

FROM {{source ('bronze','myc_taxi')}}
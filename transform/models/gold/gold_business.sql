SELECT
pickup_date,
count(*) as num_trips,
sum(trip_miles) as total_miles,
sum(trip_time) as total_time,
sum(base_passenger_fare) as total_base_passenger_fare,
sum(tolls) as total_tolls,
sum(tips) as total_tips,
sum(driver_pay) as total_driver_pay
FROM {{ref('sil_clean')}}

GROUP BY pickup_date
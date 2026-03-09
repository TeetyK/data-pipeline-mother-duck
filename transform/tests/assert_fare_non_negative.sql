
{{ config(severity= 'error')}}

SELECT *
FROM {{ ref('myc_taxi')}}
WHERE
    base_passenger_fare < 0
    OR tolls < 0
    OR tips < 0
    OR driver_pay < 0
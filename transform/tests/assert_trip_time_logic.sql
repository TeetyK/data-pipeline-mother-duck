
{{ config(severity= 'error')}}

SELECT *
FROM {{ ref('myc_taxi')}}
WHERE
    request_ts > pickup_ts
    OR pickup_ts > dropoff_ts
    OR on_scene_ts > pickup_ts
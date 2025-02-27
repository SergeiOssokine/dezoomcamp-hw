{{ config(materialized="table") }}

with
    tp_dr as (
        select
            pickup_locationid,
            dropoff_locationid,
            pickup_month,
            pickup_year,
            pickup_zone,
            dropoff_zone,
            timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_druation
        from {{ ref("dim_fhv_trips") }}
    )

select distinct
    pickup_year,
    pickup_month,
    pickup_zone,
    dropoff_zone,
    percentile_cont(tp_dr.trip_druation, 0.9) over (
        partition by pickup_year, pickup_month, pickup_locationid, dropoff_locationid
    ) as trip_duration_p90
from tp_dr

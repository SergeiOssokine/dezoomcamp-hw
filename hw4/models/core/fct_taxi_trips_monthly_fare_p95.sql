{{ config(materialized="table") }}
select distinct
    service_type,
    pickup_year,
    pickup_month,
    percentile_cont(fare_amount, 0.9) over (
        partition by service_type, pickup_year, pickup_month
    ) as p90,
    percentile_cont(fare_amount, 0.95) over (
        partition by service_type, pickup_year, pickup_month
    ) as p95,
    percentile_cont(fare_amount, 0.97) over (
        partition by service_type, pickup_year, pickup_month
    ) as p97
from {{ ref("fact_trips") }}
where
    fare_amount > 0
    and trip_distance > 0
    and payment_type_description in ('Cash', 'Credit card')
order by pickup_year, pickup_month

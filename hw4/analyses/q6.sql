select service_type, pickup_year, p90, p95, p97
from {{ ref("fct_taxi_trips_monthly_fare_p95") }}
where pickup_year = 2020 and pickup_month = 4

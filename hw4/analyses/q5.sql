select
    pickup_year,
    pickup_quarter,
    yoy_yellow,
    rank() over (order by yoy_yellow asc) as rank_yellow,
    yoy_green,
    rank() over (order by yoy_green asc) as rank_green,
from {{ ref("fct_taxi_trips_quarterly_revenue") }}
where pickup_year = 2020
order by pickup_quarter

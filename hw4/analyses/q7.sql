with
    ranking as (
        select
            pickup_year,
            pickup_month,
            pickup_zone,
            dropoff_zone,
            rank() over (partition by pickup_zone order by trip_duration_p90 desc) rnk,
            trip_duration_p90
        from {{ ref("fct_fhv_monthly_zone_traveltime_p90") }}
        where
            pickup_year = 2019
            and pickup_month = 11
            and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
    )

select pickup_zone, dropoff_zone, rnk, trip_duration_p90
from ranking
where rnk = 2
order by pickup_zone

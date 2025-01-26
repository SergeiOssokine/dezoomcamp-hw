select
    lpep_pickup_datetime::date as pickup_date,
    max(trip_distance) as max_distance
from
    green_trip_data
where
    lpep_pickup_datetime::date between '2019-10-01' and '2019-10-31'
group by pickup_date
order by max_distance desc
limit 1

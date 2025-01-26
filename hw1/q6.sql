with tmp as (
    select
        gtd.tip_amount,
        zpu."Zone" as pickup_loc,
        zdo."Zone" as dropoff_loc
    from green_trip_data as gtd
    inner join zones as zpu on gtd."PULocationID" = zpu."LocationID"
    inner join zones as zdo on gtd."DOLocationID" = zdo."LocationID"
    where
        gtd.lpep_dropoff_datetime::date between '2019-10-01' and '2019-10-31'
        and zpu."Zone" = 'East Harlem North'
)

select
    dropoff_loc,
    max(tip_amount) as max_tip
from tmp
group by dropoff_loc
order by max_tip desc
limit 1

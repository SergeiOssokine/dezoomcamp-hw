with tmp as (
    select
        gtd.lpep_pickup_datetime,
        z."Zone" as trip_zone,
        gtd.total_amount
    from green_trip_data as gtd
    inner join zones as z on gtd."PULocationID" = z."LocationID"
    where gtd.lpep_pickup_datetime::date = '2019-10-18'
)

select
    trip_zone,
    sum(total_amount) as overall_total
from tmp
group by trip_zone
having (sum(total_amount) > 13000)
order by overall_total desc

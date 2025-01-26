select
    COUNT(*) filter (where trip_distance <= 1) as dist_1,
    COUNT(*) filter (
        where trip_distance > 1 and trip_distance <= 3
    ) as dist_1_to_3,
    COUNT(*) filter (
        where trip_distance > 3 and trip_distance <= 7
    ) as dist_3_to_7,
    COUNT(*) filter (
        where trip_distance > 7 and trip_distance <= 10
    ) as dist_7_to_10,
    COUNT(*) filter (where trip_distance > 10) as dist_10
from green_trip_data
where
    lpep_dropoff_datetime::date between '2019-10-01' and '2019-10-31'

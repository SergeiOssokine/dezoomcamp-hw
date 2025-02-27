{{ config(materialized="table") }}

with dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')

select
    s.dispatching_base_num,
    s.pickup_datetime,
    s.dropoff_datetime,
    extract(year from s.pickup_datetime) as pickup_year,
    extract(month from s.pickup_datetime) as pickup_month,
    s.pickup_locationid,
    s.dropoff_locationid,
    s.sr_flag,
    s.affiliated_base_number,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.zone as dropoff_zone
from {{ ref("stg_fhv_data") }} s
inner join dim_zones as pickup_zone on s.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone on s.dropoff_locationid = dropoff_zone.locationid

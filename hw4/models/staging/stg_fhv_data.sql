{{ config(materialized="view") }}

select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
    as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
    as dropoff_locationid,
    sr_flag,
    affiliated_base_number

from {{ source("staging", "fhv_data") }}
where dispatching_base_num is not null

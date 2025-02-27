{{ config(materialized="table") }}

with
    quarterly_income_green as (
        select pickup_year, pickup_quarter, sum(total_amount) as revenue
        from {{ ref("fact_trips") }}
        where service_type = "Green"
        group by pickup_year, pickup_quarter
        having pickup_year between 2019 and 2020

        order by pickup_year, pickup_quarter
    ),

    yoy_temp_green as (
        select
            pickup_year,
            pickup_quarter,
            revenue,
            lag(revenue, 4) over (
                order by pickup_year, pickup_quarter
            ) as revenue_last_quarter,
        from quarterly_income_green
        order by pickup_year, pickup_quarter
    ),
    quarterly_income_yellow as (
        select pickup_year, pickup_quarter, sum(total_amount) as revenue
        from {{ ref("fact_trips") }}
        where service_type = "Yellow"
        group by pickup_year, pickup_quarter
        having pickup_year between 2019 and 2020

        order by pickup_year, pickup_quarter
    ),

    yoy_temp_yellow as (
        select
            pickup_year,
            pickup_quarter,
            revenue as revenue_yellow,
            lag(revenue, 4) over (
                order by pickup_year, pickup_quarter
            ) as revenue_yellow_last_quarter,
        from quarterly_income_yellow
        order by pickup_year, pickup_quarter
    )

select
    y.pickup_year,
    y.pickup_quarter,
    (y.revenue_yellow - y.revenue_yellow_last_quarter)
    / y.revenue_yellow_last_quarter
    * 100 as yoy_yellow,
    (g.revenue - g.revenue_last_quarter) / g.revenue_last_quarter * 100 as yoy_green
from yoy_temp_green g
inner join
    yoy_temp_yellow y
    on ((g.pickup_year = y.pickup_year) and (g.pickup_quarter = y.pickup_quarter))

order by y.pickup_year, y.pickup_quarter

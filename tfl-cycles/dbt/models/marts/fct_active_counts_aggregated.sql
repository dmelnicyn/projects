-- models/marts/fct_active_counts_aggregated.sql

{{
    config(
        materialized='view'
    )
}}

with base_filtered as (
    select
        site_id,
        wave,
        road_type,
        functional_area,
        borough,
        round,
        count,
        time,
        day,
        longitude,
        latitude,
        direction
    from {{ ref('fct_active_counts_with_dimensions') }}
    where
        day = 'Weekday'
        and time >= time '06:00:00'
        and time <  time '22:00:00'
),

daily_totals as (
    select
        site_id,
        wave,
        round,
        road_type,
        functional_area,
        borough,
        longitude,
        latitude,
        direction,
        sum(count) as weekday_daily_total_flow
    from base_filtered
    group by site_id, wave, round, road_type, functional_area, borough, longitude, latitude, direction
),

average_totals as (
    select
        -- Extract year from wave, assumes format like '2019 Q2 spring (Apr-Jun)'
        cast(substr(wave, 1, 4) as int64) as year,
        wave,
        site_id,
        road_type,
        functional_area,
        borough,
        longitude,
        latitude,
        direction,
        avg(weekday_daily_total_flow) as avg_weekday_daily_flow
    from daily_totals
    group by wave, site_id, road_type, functional_area, borough, longitude, latitude, direction
)

select * from average_totals
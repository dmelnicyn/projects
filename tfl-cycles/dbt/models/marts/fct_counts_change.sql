-- models/marts/fct_counts_change.sql

{{
    config(
        materialized='view'
    )
}}

with base as (
    select
        site_id,
        wave,
        avg_weekday_daily_flow,
        -- Extract year from wave, assumes format like '2019 Q2 spring (Apr-Jun)'
        cast(substr(wave, 1, 4) as int64) as year
    from {{ ref('fct_combined_aggr_counts') }}
),

pivoted as (
    select
        site_id,
        max(case when year = extract(year from current_date) - 1 then avg_weekday_daily_flow end) as flow_last_year,
        max(case when year = 2019 then avg_weekday_daily_flow end) as flow_2019,
        max(case when year = 2015 then avg_weekday_daily_flow end) as flow_2015,
        max(avg_weekday_daily_flow) as latest_flow
    from base
    group by site_id
),

calculated as (
    select
        site_id,
        latest_flow,
        flow_last_year,
        flow_2019,
        flow_2015,

        -- % change since previous year
        round(
            case 
                when flow_last_year is not null and flow_last_year != 0
                then (latest_flow - flow_last_year) / flow_last_year * 100
                else null
            end, 2
        ) as pct_change_since_last_year,
        -- % change since 2019
        round(
            case 
                when flow_2019 is not null and flow_2019 != 0
                then (latest_flow - flow_2019) / flow_2019 * 100
                else null
            end, 2
        ) as pct_change_since_2019,
        -- % change since 2015
        round(
            case 
                when flow_2015 is not null and flow_2015 != 0
                then (latest_flow - flow_2015) / flow_2015 * 100
                else null
            end, 2
        ) as pct_change_since_2015    from pivoted
)

select * from calculated

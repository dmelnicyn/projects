-- models/marts/fct_active_counts.sql

{{
    config(
        materialized='view'
    )
}}

select
    ac.wave,
    ac.site_id,
    cast(ac.date as date) as date,
    cast(ac.time as time) as time,
    ac.weather,
    ac.day,
    ac.round,
    ac.direction,
    ac.path,
    ac.mode,
    ac.count
from {{ ref('stg_active_counts') }} ac
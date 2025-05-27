-- models/marts/fct_cycleways.sql

{{
    config(
        materialized='view'
    )
}}

select
    cw.wave,
    cw.site_id,
    cast(cw.date as date) as date,
    cast(cw.time as time) as time,
    cw.weather,
    cw.day,
    cw.round,
    cw.direction,
    cw.path,
    cw.mode,
    cw.count
from {{ ref('stg_cycleways') }} cw

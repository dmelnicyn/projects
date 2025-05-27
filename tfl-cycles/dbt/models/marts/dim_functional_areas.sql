-- models/marts/dim_functional_areas.sql

{{
    config(
        materialized='table'
    )
}}

select 
    distinct functional_area_prefix as prefix,  -- Updated reference
    case
        when functional_area_prefix = '01' then 'Central'
        when functional_area_prefix = '02' then 'Inner'
        when functional_area_prefix = '03' then 'Outer'
        else 'Unknown'
    end as description
from {{ ref('stg_monitoring_locations') }}
where functional_area_prefix is not null


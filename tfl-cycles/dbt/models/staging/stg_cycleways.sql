-- models/staging/stg_cycleways.sql
{{ config(materialized="view") }}

with

    counts as (
        select *
        from {{ source("staging", "cycleways_external_table") }}
        where count is not null
    )
select
    cast(wave as string) as wave,
    cast(siteid as string) as site_id,
    cast(date as date) as date,
    cast(weather as string) as weather,
    cast(time as time) as time,
    cast(day as string) as day,
    cast(round as string) as round,
    cast(direction as string) as direction,
    cast(path as string) as path,
    cast(mode as string) as mode,
    cast(count as integer) as count

from counts
where mode not in ("Buses", "Cars", "Light goods vehicles (LGVs)", "Motorcycles", "Taxis", "All motor vehicles", "Coaches", "Heavy goods vehicles (HGVs)", "Medium goods vehicles (MGVs)")


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}

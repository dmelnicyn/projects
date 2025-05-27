-- models/staging/stg_active_counts.sql
{{
    config(
        materialized='view'
    )
}}

with 

counts as 
(
    select * from {{ source('staging', 'active_counts_external_table') }}
    where count is not null
)
select
    cast(Wave as string) as wave,
    cast(SiteID as string) as site_id,
    cast(Date as date) as date,
    cast(Weather as string) as weather,
    cast(Time as time) as time, 
    cast(Day as string) as day,
    cast(Round as string) as round,
    cast(Direction as string) as direction,
    cast(Path as string) as path,
    cast(Mode as string) as mode,
    cast(Count as integer) as count

from counts
where mode != "Pedestrians"
-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
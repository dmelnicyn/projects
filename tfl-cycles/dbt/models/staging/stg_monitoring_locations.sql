-- models/staging/stg_monitoring_locations.sql

{{
    config(
        materialized='view'
    )
}}

with locations as (
    select * 
    from {{ source('staging', 'monitoring_locations_external_table') }}
    where site_id is not null
)

select 
    ml.site_id,
    ml.location_description,
    ml.borough,
    split(ml.functional_area_for_monitoring, ' ')[offset(0)] as functional_area_prefix,
    split(ml.road_type, ' ')[offset(0)] as road_type_prefix,
    ml.Easting__UK_Grid_ as easting_uk_grid, -- Use backticks for column names with spaces
    ml.Northing__UK_Grid_ as northing_uk_grid, -- Use backticks for column names with spaces
    ml.latitude,
    ml.longitude
from locations ml


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
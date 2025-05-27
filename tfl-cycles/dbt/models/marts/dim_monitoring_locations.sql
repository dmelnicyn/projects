--models/marts/dim_monitoring_locations.sql

{{
    config(
        materialized='table'
    )
}}

with locations as (
    select * from {{ ref('stg_monitoring_locations') }}
),
road_types as (
    select * from {{ ref('dim_road_types') }}
),
functional_areas as (
    select * from {{ ref('dim_functional_areas') }}
)

select
    ml.site_id,
    ml.location_description,
    ml.borough,
    fa.description as functional_area,
    rt.description as road_type,
    ml.easting_uk_grid,
    ml.northing_uk_grid,
    ml.latitude,
    ml.longitude
from locations ml
left join functional_areas fa
    on ml.functional_area_prefix = fa.prefix
left join road_types rt
    on ml.road_type_prefix = rt.prefix
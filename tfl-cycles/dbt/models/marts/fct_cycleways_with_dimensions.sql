-- models/marts/fct_cycleways_with_dimensions.sql

{{
    config(
        materialized='table'
    )
}}

select
    fac.*,
    dim_ml.location_description,
    dim_ml.borough,
    dim_ml.functional_area,
    dim_ml.road_type,
    dim_ml.easting_uk_grid,
    dim_ml.northing_uk_grid,
    dim_ml.latitude,
    dim_ml.longitude
from {{ ref('fct_cycleways') }} fac
left join {{ ref('dim_monitoring_locations') }} dim_ml
    on fac.site_id = dim_ml.site_id

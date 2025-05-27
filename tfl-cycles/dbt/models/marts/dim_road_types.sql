{{
    config(
        materialized='table'
    )
}}

select 
    distinct road_type_prefix as prefix,  -- Updated reference
    case
        when road_type_prefix = '01' then 'A Road'
        when road_type_prefix = '02' then 'B Road'
        when road_type_prefix = '03' then 'Minor Road'
        when road_type_prefix = '04' then 'Local Street'
        when road_type_prefix = '05' then 'Motor vehicle-free'
        when road_type_prefix = '0' then 'Unclassified'
        else 'Unknown'
    end as description
from {{ ref('stg_monitoring_locations') }}
where road_type_prefix is not null

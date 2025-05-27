-- models/marts/fct_weekday_cycle_km_per_km.sql

with area_totals as (
    select
        functional_area,
        sum(weekday_cycle_km) as total_cycle_km,
        sum(group_length_km) as total_network_km
    from {{ ref('fct_weekday_cycle_km_volume') }}
    group by functional_area
)

select
    functional_area,
    total_cycle_km,
    total_network_km,
    total_cycle_km / total_network_km as cycle_km_per_km
from area_totals

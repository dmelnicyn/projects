-- models/marts/fct_weekday_cycle_km_volume.sql

with weekday_counts as (
    select
        site_id,
        wave,
        functional_area,
        road_type,
        count as raw_count
    from {{ ref('fct_active_counts_with_dimensions') }}
    where
        day = 'Weekday'
        and mode != 'Pedestrians'
),

site_avg_flows as (
    select
        site_id,
        functional_area,
        road_type,
        wave,
        avg(raw_count) as avg_daily_flow
    from weekday_counts
    group by site_id, functional_area, road_type, wave
),

group_avg_flows as (
    select
        functional_area,
        road_type,
        wave,
        avg(avg_daily_flow) as group_avg_flow
    from site_avg_flows
    group by functional_area, road_type, wave
),

lengths as (
    select
        functional_area,
        road_type,
        length / 1000.0 as group_length_km
    from {{ ref('cleaned_group_length') }}
)

select
    gaf.wave,
    gaf.functional_area,
    gaf.road_type,
    gaf.group_avg_flow,
    l.group_length_km,
    gaf.group_avg_flow * l.group_length_km as weekday_cycle_km
from group_avg_flows gaf
left join lengths l
  on gaf.functional_area = l.functional_area
 and gaf.road_type = l.road_type

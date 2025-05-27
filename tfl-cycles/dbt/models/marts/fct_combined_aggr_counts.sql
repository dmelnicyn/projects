-- models/marts/fct_combined_aggr_counts_filtered.sql

{{
    config(
        materialized='view'
    )
}}

with unioned as (
    select * from {{ ref('fct_cycleways_counts_aggregated') }}
    union all
    select * from {{ ref('fct_active_counts_aggregated') }}
),

-- Filter out rows with null functional_area
filtered as (
    select *
    from unioned
    where functional_area is not null
    
),
-- Step 1: Extract season from wave string (e.g., "2019 Q2 spring (Apr-Jun)" → "spring")
with_season as (
    select *,
        regexp_extract(lower(wave), r'(spring|summer|autumn|winter)') as season
    from filtered
),

-- Step 2: Count rows per site_id + season
season_counts as (
    select
        site_id,
        season,
        count(*) as season_count
    from with_season
    group by site_id, season
),

-- Step 3: Rank seasons per site by row count (we only want top 1)
ranked as (
    select
        sc.site_id,
        sc.season,
        sc.season_count,
        row_number() over (partition by sc.site_id order by sc.season_count desc) as season_rank
    from season_counts sc
),

-- Step 4: Join back with full data and filter to the top-ranked season per site
final as (
    select ws.*
    from with_season ws
    join ranked r
      on ws.site_id = r.site_id
     and ws.season = r.season
    where r.season_rank = 1
)

select * from final

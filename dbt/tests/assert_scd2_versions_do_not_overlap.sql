-- Every type 2 dimension must have non-overlapping validity windows per
-- natural key; otherwise the as-of join in fct_stop_arrival would duplicate
-- fact rows. Returns any overlapping pair.
with routes as (
    select 'dim_route' as dim, route_id as natural_key, valid_from, valid_to
    from {{ ref('dim_route') }}
),
stops as (
    select 'dim_stop' as dim, stop_id as natural_key, valid_from, valid_to
    from {{ ref('dim_stop') }}
),
all_versions as (
    select * from routes
    union all
    select * from stops
)

select a.dim, a.natural_key, a.valid_from, a.valid_to, b.valid_from as other_from, b.valid_to as other_to
from all_versions a
join all_versions b
  on  a.dim = b.dim
  and a.natural_key = b.natural_key
  and a.valid_from < b.valid_from
  and a.valid_to   > b.valid_from

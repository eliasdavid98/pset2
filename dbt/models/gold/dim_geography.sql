select distinct
    geography as geography_key,
    geography as geography_name
from {{ ref('silver_all_observations') }}
where geography is not null

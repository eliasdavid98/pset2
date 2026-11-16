select distinct
    gender as gender_key,
    gender as gender_name
from {{ ref('silver_all_observations') }}
where gender is not null

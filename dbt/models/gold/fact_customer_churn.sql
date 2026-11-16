with observations as (
    select
        *,
        case
            when age between 18 and 29 then '18-29'
            when age between 30 and 39 then '30-39'
            when age between 40 and 49 then '40-49'
            when age between 50 and 100 then '50+'
        end as age_band_key
    from {{ ref('silver_all_observations') }}
)
select
    observations.id as observation_id,
    observations.source_split,
    observations.customer_id,
    observations.customer_id_raw,
    observations.surname,
    geography.geography_key,
    gender.gender_key,
    age_band.age_band_key,
    product_count.product_count_key,
    observations.credit_score,
    observations.age,
    observations.tenure,
    observations.balance,
    observations.has_credit_card,
    observations.is_active_member,
    observations.estimated_salary,
    observations.exited
from observations
left join {{ ref('dim_geography') }} as geography
    on observations.geography = geography.geography_key
left join {{ ref('dim_gender') }} as gender
    on observations.gender = gender.gender_key
left join {{ ref('dim_age_band') }} as age_band
    on observations.age_band_key = age_band.age_band_key
left join {{ ref('dim_product_count') }} as product_count
    on observations.number_of_products = product_count.product_count_key

select distinct
    number_of_products as product_count_key,
    number_of_products
from {{ ref('silver_all_observations') }}
where number_of_products is not null

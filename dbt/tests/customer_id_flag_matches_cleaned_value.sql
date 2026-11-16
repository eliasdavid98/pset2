select *
from {{ ref('silver_all_observations') }}
where (customer_id_is_valid and customer_id is null)
   or (not customer_id_is_valid and customer_id is not null)

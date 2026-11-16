with typed as (
    select
        *,
        customer_id_numeric is not null
            and customer_id_numeric = trunc(customer_id_numeric, 0) as customer_id_is_valid
    from {{ ref('bronze_train') }}
)
select
    id,
    case when customer_id_is_valid then trunc(customer_id_numeric, 0) end as customer_id,
    customer_id_raw,
    customer_id_is_valid,
    surname,
    credit_score,
    initcap(geography) as geography,
    initcap(gender) as gender,
    age,
    tenure,
    balance,
    number_of_products,
    has_credit_card,
    is_active_member,
    estimated_salary,
    exited,
    'train' as source_split
from typed

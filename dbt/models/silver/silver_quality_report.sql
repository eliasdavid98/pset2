with observations as (
    select * from {{ ref('silver_all_observations') }}
),
checks as (
    select
        source_split,
        'missing_required_attributes' as check_name,
        sum(iff(
            id is null
            or surname is null
            or geography is null
            or gender is null
            or credit_score is null
            or age is null
            or tenure is null
            or balance is null
            or number_of_products is null
            or has_credit_card is null
            or is_active_member is null
            or estimated_salary is null
            or (source_split = 'train' and exited is null),
            1, 0
        )) as affected_rows,
        count(*) as total_rows
    from observations
    group by source_split

    union all

    select
        source_split,
        'invalid_customer_id' as check_name,
        sum(iff(not customer_id_is_valid, 1, 0)) as affected_rows,
        count(*) as total_rows
    from observations
    group by source_split

    union all

    select
        source_split,
        'estimated_salary_above_train_max_review' as check_name,
        sum(iff(estimated_salary > 199992.48, 1, 0)) as affected_rows,
        count(*) as total_rows
    from observations
    group by source_split
)
select
    source_split,
    check_name,
    affected_rows,
    total_rows,
    round(100.0 * affected_rows / nullif(total_rows, 0), 4) as affected_percent
from checks

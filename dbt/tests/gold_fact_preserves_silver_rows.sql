with expected as (
    select count(*) as row_count
    from {{ ref('silver_all_observations') }}
),
actual as (
    select count(*) as row_count
    from {{ ref('fact_customer_churn') }}
)
select expected.row_count as expected_rows, actual.row_count as actual_rows
from expected
cross join actual
where expected.row_count <> actual.row_count

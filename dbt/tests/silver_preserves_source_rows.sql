with expected as (
    select
        (select count(*) from {{ source('raw', 'train') }})
        + (select count(*) from {{ source('raw', 'test') }}) as row_count
),
actual as (
    select count(*) as row_count
    from {{ ref('silver_all_observations') }}
)
select expected.row_count as expected_rows, actual.row_count as actual_rows
from expected
cross join actual
where expected.row_count <> actual.row_count

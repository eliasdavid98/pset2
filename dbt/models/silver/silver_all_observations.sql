select * from {{ ref('silver_train') }}
union all
select * from {{ ref('silver_test') }}

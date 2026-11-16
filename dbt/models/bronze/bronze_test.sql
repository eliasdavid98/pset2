select
    try_to_decimal(id, 38, 9) as id,
    customerid as customer_id_raw,
    try_to_decimal(customerid, 38, 9) as customer_id_numeric,
    nullif(trim(surname), '') as surname,
    try_to_decimal(creditscore, 38, 9) as credit_score,
    nullif(trim(geography), '') as geography,
    nullif(trim(gender), '') as gender,
    try_to_decimal(age, 38, 9) as age,
    try_to_decimal(tenure, 38, 9) as tenure,
    try_to_decimal(balance, 18, 2) as balance,
    try_to_decimal(numofproducts, 38, 9) as number_of_products,
    try_to_decimal(hascrcard, 38, 9) as has_credit_card,
    try_to_decimal(isactivemember, 38, 9) as is_active_member,
    try_to_decimal(estimatedsalary, 18, 2) as estimated_salary,
    cast(null as number(38, 9)) as exited
from {{ source('raw', 'test') }}

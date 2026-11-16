select *
from {{ ref('silver_all_observations') }}
where credit_score is null or credit_score not between 300 and 850
   or credit_score <> trunc(credit_score, 0)
   or id is null or id <> trunc(id, 0)
   or age is null or age not between 18 and 100 or age <> trunc(age, 0)
   or tenure is null or tenure not between 0 and 10 or tenure <> trunc(tenure, 0)
   or balance is null or balance < 0
   or estimated_salary is null or estimated_salary < 0
   or number_of_products is null
   or number_of_products not in (1, 2, 3, 4)
   or number_of_products <> trunc(number_of_products, 0)
   or has_credit_card is null or has_credit_card not in (0, 1)
   or is_active_member is null or is_active_member not in (0, 1)
   or geography is null or geography not in ('France', 'Germany', 'Spain')
   or gender is null or gender not in ('Female', 'Male')
   or (source_split = 'train' and (exited is null or exited not in (0, 1)))
   or (source_split = 'test' and exited is not null)

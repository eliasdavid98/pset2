select train.id
from {{ ref('silver_train') }} as train
inner join {{ ref('silver_test') }} as test using (id)


-- Use the `ref` function to select from other models

select *
from {{ ref('OPP_AND_ACC_STG') }}

-- Type-2 customer dimension. Carries the FULL SCD2 history (no
-- current-only filter): a PIT rewrite of this model adds a validity-window
-- predicate and gets the row that was true at the requested time.
-- Downstream models pick current rows via is_current.
select
    customer_id,
    customer_name,
    segment,
    region,
    dbt_valid_from,
    dbt_valid_to,
    (dbt_valid_to is null) as is_current
from {{ ref('stg_customers') }}

-- History-preserving staging: reads the SCD2 snapshot, not the mutable
-- raw.customers table. History lives in ROWS (dbt_valid_from /
-- dbt_valid_to), so it survives any storage-level VACUUM — scenario S6
-- proves this against a really-vacuumed Delta mirror of the same table.
select
    customer_id,
    customer_name,
    segment,
    region,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('customers_snapshot') }}

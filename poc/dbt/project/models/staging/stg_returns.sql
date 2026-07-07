-- Staging over the Iceberg-backed source. Ephemeral, same rationale as
-- stg_orders: the physical relation must be visible in compiled SQL for
-- point-in-time binding to reach it.
select
    return_id,
    order_id,
    cast(return_date as date)    as return_date,
    cast(refund_amount as double) as refund_amount,
    reason
from {{ source('raw', 'returns') }}

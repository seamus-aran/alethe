-- Staging over the Delta-backed source. Ephemeral: this SELECT is inlined
-- into downstream compiled SQL, so `pit_poc.raw.orders` appears verbatim
-- where the alethe PIT rewriter / macro shim can bind it.
select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    cast(amount as double)   as amount
from {{ source('raw', 'orders') }}

-- Fact: order grain, joined to returns for net revenue.
-- Upstream leaves (what alethe watermarks):
--   * source raw.orders  -> delta://orders        (real Delta history)
--   * source raw.returns -> iceberg://raw.returns (real Iceberg history)
-- Because staging is ephemeral, the compiled SQL of this model references
-- both physical source tables directly — a PIT rewrite binds them both.
select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    r.return_id,
    r.return_date,
    coalesce(r.refund_amount, 0) as refund_amount,
    o.amount - coalesce(r.refund_amount, 0) as net_amount
from {{ ref('stg_orders') }} o
left join {{ ref('stg_returns') }} r
  on o.order_id = r.order_id

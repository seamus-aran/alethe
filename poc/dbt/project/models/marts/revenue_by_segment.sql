-- Mart on the fact/dim layer: net revenue per segment per calendar day.
-- Note for PIT rewriting: this model reads MATERIALIZED fact/dim tables,
-- so its own compiled SQL contains no physical leaf relations — binding
-- must happen where the leaves appear (fct_orders, dim_customer). The
-- notebook demonstrates this deliberately (S4).
select
    d.date_day,
    d.is_weekend,
    c.segment,
    count(*)               as order_count,
    sum(f.amount)          as gross_revenue,
    sum(f.refund_amount)   as refunds,
    sum(f.net_amount)      as net_revenue
from {{ ref('fct_orders') }} f
join {{ ref('dim_customer') }} c
  on f.customer_id = c.customer_id
 and c.is_current
join {{ ref('dim_date') }} d
  on f.order_date = d.date_day
group by 1, 2, 3
order by 1, 3

select
    restaurant_id,
    date_trunc('day', window_start) as order_date,
    sum(order_count) as total_orders,
    sum(canceled_count) as total_canceled,
    round(sum(canceled_count)::float / nullif(sum(order_count), 0), 4) as cancel_rate,
    round(avg(avg_order_value), 2) as avg_order_value,
    round(avg(avg_eta_minutes), 1) as avg_eta_minutes
from {{ ref('stg_restaurant_ops_metrics') }}
group by 1, 2

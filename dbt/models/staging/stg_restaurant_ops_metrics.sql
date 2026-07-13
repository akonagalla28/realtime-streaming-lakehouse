with source as (
    select * from {{ source('lakehouse_gold', 'restaurant_ops_metrics') }}
)

select
    restaurant_id,
    window_start,
    window_end,
    order_count,
    avg_order_value,
    canceled_count,
    cancel_rate,
    avg_eta_minutes
from source
where order_count > 0

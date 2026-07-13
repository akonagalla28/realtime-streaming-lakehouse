with source as (
    select * from {{ source('lakehouse_gold', 'eta_model_features') }}
)

select
    order_id,
    restaurant_id,
    driver_id,
    eta_minutes,
    driver_active_orders,
    restaurant_open_orders,
    restaurant_on_time_rate_rolling,
    event_ts
from source
where order_id is not null

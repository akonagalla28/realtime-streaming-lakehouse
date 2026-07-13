{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='append_new_columns'
    )
}}

select
    f.order_id,
    f.restaurant_id,
    f.driver_id,
    f.eta_minutes,
    f.driver_active_orders,
    f.restaurant_open_orders,
    f.restaurant_on_time_rate_rolling,
    r.cancel_rate as restaurant_recent_cancel_rate,
    r.avg_eta_minutes as restaurant_recent_avg_eta,
    f.event_ts
from {{ ref('stg_eta_model_features') }} f
left join {{ ref('stg_restaurant_ops_metrics') }} r
    on f.restaurant_id = r.restaurant_id
    and f.event_ts between r.window_start and r.window_end

{% if is_incremental() %}
where f.event_ts > (select coalesce(max(event_ts), '1900-01-01') from {{ this }})
{% endif %}

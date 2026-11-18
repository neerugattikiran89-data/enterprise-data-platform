{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_customers') }} c
    ON o.customer_id = c.customer_id

{% if is_incremental() %}
WHERE o.order_purchase_timestamp > (
    SELECT COALESCE(MAX(order_purchase_timestamp), '')
    FROM {{ this }}
)
{% endif %}

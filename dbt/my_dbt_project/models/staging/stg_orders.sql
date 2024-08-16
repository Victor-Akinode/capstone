WITH raw_orders AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    FROM 
        `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.orders`
)

SELECT * FROM raw_orders;
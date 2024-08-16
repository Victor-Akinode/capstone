WITH delivery_times AS (
    SELECT 
        o.order_id,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        TIMESTAMP_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, HOUR) AS delivery_time_hours
    FROM 
        `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.orders` o
    WHERE 
        o.order_status = 'delivered'
)

SELECT 
    AVG(delivery_time_hours) AS avg_delivery_time_hours
FROM 
    delivery_times;
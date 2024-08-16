SELECT 
    customer_state,
    total_orders
FROM 
    `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.int_orders_by_state`;
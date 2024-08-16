SELECT 
    avg_delivery_time_hours
FROM 
    `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.int_avg_delivery_time`;
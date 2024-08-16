SELECT 
    product_category_name,
    total_sales
FROM 
    `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.int_sales_by_category`;
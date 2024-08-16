WITH sales_data AS (
    SELECT 
        p.product_category_name,
        SUM(oi.price) AS total_sales
    FROM 
        `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.order_items` oi
    JOIN 
        `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.products` p 
    ON 
        oi.product_id = p.product_id
    GROUP BY 
        p.product_category_name
)

SELECT * FROM sales_data;
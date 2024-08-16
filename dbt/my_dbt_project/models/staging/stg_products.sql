WITH raw_products AS (
    SELECT 
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    FROM 
        `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.products`
)

SELECT * FROM raw_products;
WITH orders_by_state AS (
    SELECT 
        c.customer_state,
        COUNT(o.order_id) AS total_orders
    FROM 
        `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.orders` o
    JOIN 
        `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.customers` c 
    ON 
        o.customer_id = c.customer_id
    GROUP BY 
        c.customer_state
)

SELECT * FROM orders_by_state;
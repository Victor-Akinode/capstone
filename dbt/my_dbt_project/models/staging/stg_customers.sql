with customers_raw as (
    select * 
    from `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.olist_customers_dataset`
)

select 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from customers_raw

with delivery_data as (
    select 
        order_id,
        customer_id,
        order_delivered_customer_date,
        order_purchase_timestamp,
        date_diff(order_delivered_customer_date, order_purchase_timestamp, day) as delivery_time
    from `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.orders`
    where order_delivered_customer_date is not null
)

select 
    avg(delivery_time) as average_delivery_time
from delivery_data

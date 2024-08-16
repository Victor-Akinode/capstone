with orders_data as (
    select 
        c.customer_state,
        count(o.order_id) as number_of_orders
    from `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.olist_orders_dataset` as o
    join `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.olist_customers_dataset` as c
        on o.customer_id = c.customer_id
    group by c.customer_state
)

select 
    customer_state,
    number_of_orders
from orders_data
order by number_of_orders desc

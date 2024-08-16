with sales_data as (
    select 
        p.product_category_name,
        sum(oi.price) as total_sales
    from `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.olist_order_items_dataset` as oi
    join `{{ env_var('PROJECT_ID') }}.{{ env_var('DATASET_ID') }}.olist_products_dataset` as p
        on oi.product_id = p.product_id
    group by p.product_category_name
)

select 
    product_category_name,
    total_sales
from sales_data
order by total_sales desc

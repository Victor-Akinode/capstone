Project Overview

This project involves building an end-to-end data pipeline for analyzing Brazilian e-commerce data. The pipeline extracts raw data from PostgreSQL, transforms it using dbt, and loads it into Google BigQuery for further analysis. Apache Airflow is used to orchestrate the ETL (Extract, Transform, Load) process, ensuring a seamless data flow. The project also answers key analytical questions, such as determining the highest sales by product category, the average delivery time, and the number of orders by state.

Project Structure
capstone-project/
├── dags/
│   ├── etl_postgres_to_bigquery.py
│   └── other_dags_files.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_products.sql
│   │   │   └── other_staging_models.sql
│   │   ├── intermediate/
│   │   │   ├── int_sales_by_category.sql
│   │   │   ├── int_avg_delivery_time.sql
│   │   │   └── int_orders_by_state.sql
│   │   ├── final/
│   │   │   ├── fct_sales_by_category.sql
│   │   │   ├── fct_avg_delivery_time.sql
│   │   │   └── fct_orders_by_state.sql
│   ├── analyses/
│   │   │   ├── product_category_sales.sql
│   │   │   ├── average_delivery_time.sql
│   │   │   └── orders_per_state.sql
│   └── other_models/
├── data/
│   ├── olist_customers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_products_dataset.csv
│   └── other_data_files.csv
├── docker-compose.yml
├── scripts/
│   ├── clean.py
│   ├── ingest_data.py
│   ├── init.sql
│   └── other_scripts.py
└── .env



Technologies Used

	•	Apache Airflow: For orchestrating the ETL pipeline.
	•	Google BigQuery: For data warehousing and analysis.
	•	dbt (Data Build Tool): For transforming and modeling data.
	•	PostgreSQL: As the initial data source.
	•	Docker: For containerization and environment setup.
	•	Python: For scripting and data processing.

Setup Instructions

1. Prerequisites

	•	Docker and Docker Compose installed on your machine.
	•	A Google Cloud Platform (GCP) project with BigQuery enabled.
	•	Python 3.9 installed.

2. Clone the Repository
git clone https://github.com/Victor-Akinode/capstone
cd capstone

3. Environment Variables Setup

Create a .env file in the root directory with the following:
POSTGRES_DB=your_postgres_db
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_HOST=db
POSTGRES_PORT=5432

GCS_BUCKET=your-gcs-bucket
GCS_PATH=path/to/your/folder
BQ_DATASET=your_bigquery_dataset
BQ_TABLE=your_bigquery_table

4. Docker Setup

Start all necessary services using Docker Compose:
docker-compose up -d

This will start PostgreSQL, Redis, and Airflow services.

5. Running Airflow

To access the Airflow UI, navigate to http://localhost:8080.

Use the following credentials to log in:

	•	Username: airflow
	•	Password: airflow

6. Setting Up dbt

Navigate to the dbt_project directory:
cd dbt_project

Install dbt:
pip install dbt-bigquery

Initialize the dbt project:
dbt init

Data Ingestion Process
1. Clean and Transform Raw Data

Use the clean.py script to clean the raw data:
python scripts/clean.py

This script handles missing values and other inconsistencies in the data.

2. Ingest Data into PostgreSQL

Run the ingest_data.py script to load the cleaned data into PostgreSQL:
python scripts/ingest_data.py

ETL Pipeline with Apache Airflow

1. Extract Data from PostgreSQL

The ETL pipeline is defined in the etl_postgres_to_bigquery.py file within the dags/ directory. This script extracts data from PostgreSQL, processes it, and prepares it for loading into BigQuery.

2. Load Data into Google BigQuery

The ETL pipeline loads the extracted and processed data into the specified BigQuery dataset and table.

Data Modeling with dbt

1. Staging Models

The staging folder contains SQL scripts to clean and standardize raw data from BigQuery. Each .sql file corresponds to a specific data entity, such as stg_orders.sql, stg_products.sql, etc.

2. Intermediate Models

The intermediate folder contains SQL scripts that transform staged data into intermediate aggregates. These include int_sales_by_category.sql, int_avg_delivery_time.sql, and int_orders_by_state.sql.

3. Final Models

The final folder contains SQL scripts that provide final transformed data for analysis, such as fct_sales_by_category.sql, fct_avg_delivery_time.sql, and fct_orders_by_state.sql.

Answering Analytical Questions

1. Highest Sales by Product Category

The product_category_sales.sql script aggregates sales data by product category.

2. Average Delivery Time

The average_delivery_time.sql script calculates the average time taken for orders to be delivered.

3. Number of Orders by State

The orders_per_state.sql script counts the total number of orders for each state.

Conclusion

This project demonstrates the full pipeline of data extraction, transformation, and loading using Apache Airflow, Google BigQuery, and dbt. The final analytical models provide valuable insights into the Brazilian e-commerce landscape.

References

	•	Apache Airflow Documentation
	•	Google BigQuery Documentation
	•	dbt Documentation


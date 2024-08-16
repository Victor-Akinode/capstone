from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

# Load environment variables
GCS_BUCKET = os.getenv('GCS_BUCKET')
GCS_PATH = os.getenv('GCS_PATH')
BQ_DATASET = os.getenv('BQ_DATASET')
BQ_TABLE = os.getenv('BQ_TABLE')

# Define default_args and DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'postgres_to_bigquery',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

# List of tables to export
tables = ['order_reviews_data', 'other_tables']  # Add all relevant tables here

def extract_data_from_postgres(table, dst):
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_connection_id')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    query = f"SELECT * FROM {table}"
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    df.to_csv(dst, index=False)
    cursor.close()
    conn.close()

# Loop through each table to create the tasks
for table in tables:
    export_task = PythonOperator(
        task_id=f'export_{table}_to_gcs',
        python_callable=extract_data_from_postgres,
        op_kwargs={
            'table': table,
            'dst': f'/tmp/{table}_{{{{ ds_nodash }}}}.csv',  # Save locally first
        },
        dag=dag,
    )

    upload_task = LocalFilesystemToGCSOperator(
        task_id=f'upload_{table}_to_gcs',
        src=f'/tmp/{table}_{{{{ ds_nodash }}}}.csv',
        dst=f'{GCS_PATH}/{table}_{{{{ ds_nodash }}}}.csv',
        bucket=GCS_BUCKET,
        dag=dag,
    )

    load_to_bq_task = GCSToBigQueryOperator(
        task_id=f'load_{table}_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[f'{GCS_PATH}/{table}_{{{{ ds_nodash }}}}.csv'],
        destination_project_dataset_table=f'{BQ_DATASET}.{table}',
        schema_fields=None,  # Optional: Add schema if necessary
        write_disposition='WRITE_TRUNCATE',
        dag=dag,
    )

    # Define task dependencies
    export_task >> upload_task >> load_to_bq_task
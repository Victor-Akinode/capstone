from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime

def extract_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pg_hook.get_pandas_df('SELECT * FROM customers')
    df.to_csv('/tmp/customers.csv', index=False)

def load_data():
    bq_hook = BigQueryHook(bigquery_conn_id='bigquery_default', delegate_to=None)
    bq_hook.run_load('my_dataset.my_table', '/tmp/customers.csv')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG('etl_dag', default_args=default_args, schedule_interval='@daily')

start = DummyOperator(task_id='start', dag=dag)
extract = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
load = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> extract >> load >> end
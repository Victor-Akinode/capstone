import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from .env
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('POSTGRES_HOST')
db_port = os.getenv('POSTGRES_PORT')
db_name = os.getenv('POSTGRES_DB')

# Create a connection to the PostgreSQL database
engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Base directory for the data files
base_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')

# Function to truncate a table
def truncate_table(engine, table_name):
    with engine.connect() as conn:
        conn.execute(text(f'TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;'))
        print(f"Table {table_name} truncated.")

# Function to load a CSV file into a PostgreSQL table with ON CONFLICT DO NOTHING
def load_csv_to_table(csv_file_name, table_name):
    csv_file_path = os.path.join(base_data_dir, csv_file_name)
    df = pd.read_csv(csv_file_path)

    # Correct common typos in column names
    corrections = {
        'product_name_lenght': 'product_name_length',
        'product_description_lenght': 'product_description_length',
        # Add more corrections as necessary
    }

    # Apply corrections if those columns exist
    df.rename(columns=corrections, inplace=True)

    # Replace NaN values with None (for SQL NULL)
    df = df.where(pd.notnull(df), None)

    # Convert DataFrame to a list of dictionaries
    rows = df.to_dict(orient='records')

    # Dynamically generate the SQL statement with bind parameters
    column_names = df.columns
    insert_stmt = text(f"""
        INSERT INTO {table_name} ({', '.join(column_names)}) 
        VALUES ({', '.join([f':{col}' for col in column_names])})
        ON CONFLICT DO NOTHING;
    """)

    # Insert data row by row using the bind parameters
    with engine.connect() as conn:
        conn.execute(insert_stmt, rows)

    print(f"Data loaded into {table_name} from {csv_file_name}")

# Dictionary of table names and corresponding CSV files
csv_files = {
    'customers': 'olist_customers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'order_payments': 'olist_order_payments_dataset.csv',
    'order_reviews': 'olist_order_reviews_dataset.csv',
    'orders': 'olist_orders_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'product_category_name_translation': 'product_category_name_translation.csv'
}

# Truncate each table before loading new data
for table_name in csv_files.keys():
    truncate_table(engine, table_name)

# Load each CSV file into its corresponding table
for table_name, csv_file_name in csv_files.items():
    load_csv_to_table(csv_file_name, table_name)
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def load_csv_to_postgres():
    # Чтение CSV
    df = pd.read_csv('/opt/airflow/data/sales_data.csv', parse_dates=['transaction_date'])
    # Подключение к БД
    engine = create_engine('postgresql://airflow:airflow@postgres/airflow')
    # Запись в таблицу raw.sales (если таблица не существует, создастся)
    df.to_sql('sales', engine, schema='raw', if_exists='replace', index=False)
    print("Data loaded to raw.sales")

default_args = {
    'owner': 'student',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}

with DAG('load_sales_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    create_raw_table_if_not_exists = PostgresOperator(
        task_id='create_raw_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS raw.sales (
            transaction_id INTEGER,
            transaction_date DATE,
            customer_id INTEGER,
            product_id VARCHAR(10),
            quantity INTEGER,
            amount FLOAT
        );
        """
    )

    load_data = PythonOperator(
        task_id='load_csv_data',
        python_callable=load_csv_to_postgres
    )

    create_raw_table_if_not_exists >> load_data

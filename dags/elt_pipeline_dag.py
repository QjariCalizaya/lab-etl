from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os


def load_csv():
    csv_path = '/opt/airflow/data/sales_data.csv'

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f'No se encontró el archivo: {csv_path}')

    df = pd.read_csv(csv_path, parse_dates=['transaction_date'])

    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')

    df.to_sql(
        'sales',
        engine,
        schema='raw',
        if_exists='append',
        index=False
    )

    print("Data loaded to raw.sales")


default_args = {
    'owner': 'student',
    'start_date': datetime(2023, 1, 1),
    'retries': 1
}


with DAG(
    dag_id='elt_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    create_raw_table = PostgresOperator(
        task_id='create_raw_table',
        postgres_conn_id='postgres_default',
        sql="""
        CREATE SCHEMA IF NOT EXISTS raw;

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
        python_callable=load_csv
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/dbt && dbt run --profiles-dir /opt/dbt/profiles'
    )

    create_raw_table >> load_data >> run_dbt
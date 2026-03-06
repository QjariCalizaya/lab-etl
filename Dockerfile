FROM apache/airflow:2.7.1
RUN pip install dbt-postgres pandas sqlalchemy psycopg2-binary
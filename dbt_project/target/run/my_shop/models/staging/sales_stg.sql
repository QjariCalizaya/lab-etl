
  create view "airflow"."analytics_analytics"."sales_stg__dbt_tmp"
    
    
  as (
    select
    transaction_id,
    transaction_date::date,
    customer_id,
    product_id,
    quantity::int,
    amount::numeric(10,2)
from "airflow"."raw"."sales"
  );
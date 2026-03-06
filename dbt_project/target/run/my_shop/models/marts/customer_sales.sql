
  
    

  create  table "airflow"."analytics_analytics"."customer_sales__dbt_tmp"
  
  
    as
  
  (
    select
    customer_id,
    min(transaction_date) as first_purchase,
    max(transaction_date) as last_purchase,
    count(transaction_id) as num_transactions,
    sum(amount) as total_amount
from "airflow"."analytics_analytics"."sales_stg"
group by customer_id
  );
  
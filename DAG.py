from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

with DAG(
  'etl_twitter_pipeline',
  description="A simple twitter ETL pipeline using Python,PostgreSQL and Apache Airflow",
  start_date=datetime(year=2023, month=2, day=5),
  schedule_interval=timedelta(minutes=2)
) as dag:
  
  start_pipeline = EmptyOperator(
    task_id='start_pipeline',
  )
  
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_connection',
    sql='sql/create_table.sql'
  )
  
  
etl = PythonOperator(
    task_id = 'extract_data',
    python_callable = extract_data
  )

  
clean_table = PostgresOperator(
      task_id='clean_sql_table',
      postgres_conn_id='postgres_connection',
      sql=["""TRUNCATE TABLE twitter_etl_table"""]
  )
    
end_pipeline = EmptyOperator(
      task_id='end_pipeline',
  )
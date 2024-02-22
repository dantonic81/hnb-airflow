from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from products_etl import main

# Define your DAG settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define the function to be executed by the PythonOperator
def run_products_etl():
    main()


# Define your Airflow DAG
dag = DAG(
    'products_etl_dag',
    default_args=default_args,
    description='DAG for running products ETL',
    schedule_interval='0 0 * * *',  # Run every day at 00:00
)

# Define the PythonOperator to run your ETL script
run_etl_task = PythonOperator(
    task_id='run_products_etl',
    python_callable=run_products_etl,
    dag=dag,
)

# Set task dependencies (if any)
# For example, if you have other tasks that need to run before this one:
# run_etl_task >> another_task


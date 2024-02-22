from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from erasure_requests_etl import main

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
def run_erasure_requests_etl():
    main()


# Define your Airflow DAG
dag = DAG(
    'erasure_requests_etl_dag',
    default_args=default_args,
    description='DAG for running erasure requests ETL',
    schedule_interval='0 1 * * *',  # Run every day at 01:00
)

# Define the PythonOperator to run your ETL script
run_etl_task = PythonOperator(
    task_id='run_erasure_requests_etl',
    python_callable=run_erasure_requests_etl,
    dag=dag,
)

# Set task dependencies (if any)
# For example, if you have other tasks that need to run before this one:
# run_etl_task >> another_task


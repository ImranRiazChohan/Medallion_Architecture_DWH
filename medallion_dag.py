from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.append('/opt/airflow/medallion_project')

from bronze_layer import main as bronze_main
from silver_layer import load_silver as silver_main
from golden_layer import main as golden_main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'medallion_architecture_ingestion',
    default_args=default_args,
    description='DAG to automate the complete medallion architecture: Bronze -> Silver -> Golden',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
)

def run_bronze_layer():
    """Function to run the bronze layer ingestion."""
    try:
        bronze_main()
        print("Bronze layer ingestion completed successfully.")
    except Exception as e:
        print(f"Error during bronze layer ingestion: {e}")
        raise

def run_silver_layer():
    """Function to run the silver layer transformation."""
    try:
        silver_main()
        print("Silver layer transformation completed successfully.")
    except Exception as e:
        print(f"Error during silver layer transformation: {e}")
        raise

def run_golden_layer():
    """Function to run the golden layer aggregation."""
    try:
        golden_main()
        print("Golden layer aggregation completed successfully.")
    except Exception as e:
        print(f"Error during golden layer aggregation: {e}")
        raise

bronze_task = PythonOperator(
    task_id='ingest_bronze_layer',
    python_callable=run_bronze_layer,
    dag=dag,
)

silver_task = PythonOperator(
    task_id='transform_silver_layer',
    python_callable=run_silver_layer,
    dag=dag,
)

golden_task = PythonOperator(
    task_id='aggregate_golden_layer',
    python_callable=run_golden_layer,
    dag=dag,
)

# Set task dependencies: Bronze -> Silver -> Golden
bronze_task >> silver_task >> golden_task
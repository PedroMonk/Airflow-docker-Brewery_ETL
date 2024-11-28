"""
OpenBrewery ETL DAG

This DAG implements an ETL (Extract, Transform, Load) pipeline for the Open Brewery DB API data,
following the medallion architecture with Bronze, Silver, and Gold layers.

The pipeline consists of the following steps:
1. Extract data from the Open Brewery DB API
2. Load raw data into the Bronze layer
3. Transform and clean data in the Silver layer
4. Aggregate data in the Gold layer for analytics

Author: [Pcosta]
Date: November 2024
"""

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from load_bronze_module import extract_brewery_data, load_bronze
from transform_silver import transform_silver
from transform_gold import transform_gold

# Default arguments for all tasks in the DAG
default_args = {
    'owner': 'airflow',                 # Owner of the DAG
    'depends_on_past': False,           # Tasks don't depend on past runs
    'start_date': days_ago(1),          # Start date for the DAG (1 day ago)
    'email_on_failure': False,          # Don't send emails on failure
    'email_on_retry': False,            # Don't send emails on retry
    'retries': 1,                       # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5), # Delay between retries
}

# DAG definition
with DAG(
    'brewery_ETL',                      # DAG ID
    default_args=default_args,          # Default arguments defined above
    description='Collects and processes data from Open Brewery DB API', # DAG description
    schedule_interval='@daily',         # Run daily
    catchup=False                       # Don't backfill missing runs
) as dag:

    # Task 1: Extract data from the Open Brewery DB API
    extract_task = PythonOperator(
        task_id='extract_brewery_data',
        python_callable=extract_brewery_data
    )

    # Task 2: Load raw data into Bronze layer
    load_bronze_task = PythonOperator(
        task_id='load_bronze',
        python_callable=load_bronze
    )

    # Task 3: Transform data and load into Silver layer
    transform_silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver
    )

    # Task 4: Aggregate data and load into Gold layer
    transform_gold_task = PythonOperator(
        task_id='transform_gold',
        python_callable=transform_gold
    )

    # Define the task dependencies
    # The pipeline follows this sequence:
    # 1. Extract data
    # 2. Load to Bronze
    # 3. Transform to Silver
    # 4. Transform to Gold
    extract_task >> load_bronze_task >> transform_silver_task >> transform_gold_task
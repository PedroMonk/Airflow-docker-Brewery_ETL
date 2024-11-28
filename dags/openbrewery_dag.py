# dags/openbrewery_dag.py
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from load_bronze_module import extract_brewery_data, load_bronze
from transform_silver import transform_silver
from transform_gold import transform_gold

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'brewery_ETL',
    default_args=default_args,
    description='Coleta dados da Open Brewery DB API ',
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_brewery_data',
        python_callable=extract_brewery_data
    )

    load_bronze_task = PythonOperator(
        task_id='load_bronze',
        python_callable=load_bronze
    )
    transform_silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver
    )
    transform_gold_task = PythonOperator(
        task_id='transform_gold',
        python_callable=transform_gold
    )

    # Definir ordem das tasks
    extract_task >> load_bronze_task >> transform_silver_task >> transform_gold_task
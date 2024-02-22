from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import os

def run_dbt_docs():
    # Set the DBT project directory
    os.chdir('/appz/home/airflow/dags/dbt/jaffle_shop')
    # Path to the DBT executable
    dbt_executable_path = '/dbt_venv/bin/dbt'
    # Running the DBT docs generate command
    subprocess.run(f'{dbt_executable_path} docs generate', shell=True, check=True)

default_args = {
    'start_date': datetime(2021, 1, 1),
    "owner": "mpmathew"
}

with DAG('dbt_docs_generation_dag', default_args=default_args, schedule_interval='@daily') as dag:
    generate_dbt_docs = PythonOperator(
        task_id='generate_dbt_docs',
        schedule=None,
        tags=["mpmathew","demo"],
        catchup=False,
        python_callable=run_dbt_docs
    )

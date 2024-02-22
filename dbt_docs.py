from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess

def generate_dbt_docs():
    # Set the path to your virtual environment's activate script
    # and your dbt project directory
    dbt_dir = "/appz/home/airflow/dags/dbt/jaffle_shop"
    command = "/dbt_venv/bin/dbt docs generate"
    # Execute the dbt command
    subprocess.run(command, cwd=dbt_dir, shell=True, check=True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
}

dag = DAG('dbt_docs_generation_dag',
          default_args=default_args,
          schedule=None,
          catchup=False)

generate_docs_task = PythonOperator(
    task_id='generate_dbt_docs',
    python_callable=generate_dbt_docs,
    dag=dag,
)

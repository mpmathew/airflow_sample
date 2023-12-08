import subprocess
from airflow import DAG
from airflow.utils.db import create_session
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models import Variable
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'convert_all_variables_to_environment_variables',
    default_args=default_args,
    description='Convert all Airflow variables to OS environment variables',
    schedule_interval=timedelta(days=1),
)

def convert_all_variables_to_environment_variables(**kwargs):
    # a db.Session object is used to run queries against
    # the create_session() method will create (yield) a session
    with create_session() as session:
        # By calling .query() with Variable, we are asking the airflow db 
        # session to return all variables (select * from variables).
        # The result of this is an iterable item similar to a dict but with a 
        # slightly different signature (object.key, object.val).
        airflow_vars = {var.key: var.val for var in session.query(Variable)}
    
        Convert each Airflow variable to an OS environment variable
        for var_name in airflow_variables:
            var_value = Variable.get(var_name)
            os.environ[var_name] = var_value
    
        print("All Airflow variables converted to OS environment variables.")

convert_task = PythonOperator(
    task_id='convert_all_variables_task',
    python_callable=convert_all_variables_to_environment_variables,
    provide_context=True,
    dag=dag,
)

convert_task

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
import os

SNOWFLAKE_CONN_ID = "snowflake_connection"
SNOWFLAKE_SCHEMA = "TEST_DEV_DB.TEST_SCHEMA"

# DAG configuration
default_args = {
  "owner": "mpmathew",
  "snowflake_conn_id": SNOWFLAKE_CONN_ID
}
dag = DAG(
    'run_snowflake_sql_files_in_subdirectories',
    default_args=default_args,
    description='Run SQL files in Snowflake, organized by subdirectories',
    schedule_interval=None,
    start_date=days_ago(1),
)

# Base directory path
base_directory_path = "/appz/home/airflow/dags/dbt/jaffle_shop/objects/"

subdirectories = ['functions', 'stored_proc', 'streams']

for subdir_name in subdirectories:
    subdir_path = os.path.join(base_directory_path, subdir_name)
    
    with TaskGroup(group_id=subdir_name, dag=dag) as tg:
        prev_task = None
        
        # Ensure the directory exists before attempting to iterate over its contents
        if os.path.exists(subdir_path) and os.path.isdir(subdir_path):
            for file in sorted(os.listdir(subdir_path)):
                if file.endswith('.sql'):
                    file_path = os.path.join(subdir_path, file)
                    task_id = f"{subdir_name}_{file.replace('.sql', '')}"
                    task = SnowflakeOperator(
                        task_id=task_id,
                        sql=file_path,
                        snowflake_conn_id=SNOWFLAKE_CONN_ID,
                        params={"schema_name": SNOWFLAKE_SCHEMA},
                        dag=dag,
                        )
                        
                    if prev_task:
                        prev_task >> task
                    prev_task = task
        else:
            print(f"Directory {subdir_path} does not exist.")

Functions = dag.task_group_dict['functions']
Procedures = dag.task_group_dict['streams']
Streams = dag.task_group_dict['streams']
Functions >> Procedures >> Streams

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
import os

SNOWFLAKE_CONN_ID = "snowflake_connection"
SNOWFLAKE_SCHEMA = "TEST_DEV_DB.TEST_SCHEMA"

base_directory_path = "/appz/home/airflow/dags/dbt/jaffle_shop/objects/"
parent_dir_name = os.path.basename(os.path.dirname(os.path.dirname(base_directory_path)))
dynamic_dag_id = f"{parent_dir_name}_objects"

default_args = {
    "owner": "mpmathew",
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
}
dag = DAG(
    dynamic_dag_id,
    default_args=default_args,
    description='Run SQL files in Snowflake, organized by subdirectories',
    schedule_interval=None,
    start_date=days_ago(1),
)

target_subdirs = ['functions', 'stored_proc', 'streams']
task_groups = {}

for subdir, dirs, files in os.walk(base_directory_path):
    subdir_name = os.path.basename(subdir)
    if subdir_name not in target_subdirs or subdir == base_directory_path:
        continue
    
    with TaskGroup(group_id=subdir_name, dag=dag) as tg:
        prev_task = None
        
        for file in sorted(files):
            if file.endswith('.sql'):
                file_path = os.path.join(subdir, file)
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
        task_groups[subdir_name] = tg

if 'functions' in task_groups and 'stored_proc' in task_groups:
    task_groups['functions'] >> task_groups['stored_proc']
if 'stored_proc' in task_groups and 'streams' in task_groups:
    task_groups['stored_proc'] >> task_groups['streams']

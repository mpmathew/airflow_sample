from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import days_ago
import os

SNOWFLAKE_CONN_ID = "snowflake_connection"
SNOWFLAKE_SCHEMA = "TEST_DEV_DB.TEST_SCHEMA"

# DAG configuration
default_args = {
    "owner": "mpmathew",
    "snowflake_conn_id": SNOWFLAKE_CONN_ID,
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
target_subdirs = ['functions', 'stored_proc', 'streams']

# Create an empty dictionary to hold TaskGroup references
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
                
                with open(file_path, 'r') as f:
                    sql_query = f.read()
                    
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

# Set dependencies between TaskGroups
if 'functions' in task_groups and 'stored_proc' in task_groups:
    task_groups['functions'] >> task_groups['stored_proc']
if 'stored_proc' in task_groups and 'streams' in task_groups:
    task_groups['stored_proc'] >> task_groups['streams']

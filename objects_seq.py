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

for subdir, dirs, files in os.walk(base_directory_path):
    # Skip the base directory itself
    if subdir == base_directory_path:
        continue
    
    subdir_name = os.path.basename(subdir)
    
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
                        sql=sql_query,
                        snowflake_conn_id=SNOWFLAKE_CONN_ID,
                        params={"schema_name": SNOWFLAKE_SCHEMA},
                        dag=dag,
                    )
                    
                    if prev_task:
                        prev_task >> task  # Set dependency for sequential execution
                    
                    prev_task = task

# If you need to set dependencies between TaskGroups, you can do so outside the loop
# For example, if 'functions' should run before 'tables', you can add:
# tg_functions = dag.task_group_dict['functions']
# tg_tables = dag.task_group_dict['tables']
# tg_functions >> tg_tables

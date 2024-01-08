from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pathlib import Path

def on_failure_callback(context):
    dag_run = context.get('dag_run')
    task_instances = dag_run.get_task_instances()
    print("TASK_FAILED")

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath = "/appz/home/airflow/dags/dbt/jaffle_shop/profiles.yml",
)

def print_variable(**kwargs):
  #line changed
  variable = kwargs['dag_run'].conf.get('payment_type')
  print(variable)

with DAG(
    dag_id="jaffle_shop_new",
    start_date=datetime(2023, 11, 10),
    schedule_interval="0 0 * 1 *",
):
    e1 = PythonOperator(task_id = "print_variables",
                        python_callable = print_variable,
                        provide_context=True,
                       )

    seeds_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
    ),
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        select=["path:seeds/"],
    ),
        default_args={"retries": 2},
        group_id = "dbt_seeds_group"
    )

    stg_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
    ),
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        select=["path:models/staging/"],
    ),
        default_args={"retries": 1,
                     'on_failure_callback': on_failure_callback,},
        group_id = "dbt_stg_group"
    )

    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
    ),
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        exclude=["path:models/staging","path:seeds/"],
    ),
        default_args={"retries": 2},
    )
   
    e2 = EmptyOperator(task_id="post_dbt")
    

    e1 >> seeds_tg >> stg_tg >> dbt_tg >> e2

from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import TestBehavior
from airflow.models import Variable
from pathlib import Path
from cosmos.operators import DbtDocsOperator

AIRFLOW_USER = "airflow"
POSTGRES_TEST_PASSWORD = Variable.get("AIRFLOW_POSTGRES_TEST_PASSWORD")

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath = "/appz/home/airflow/dags/dbt/jaffle_shop/profiles.yml",
)


with DAG(
    dag_id="dbt_docs",
    start_date=datetime(2023, 11, 10),
    schedule=None,
    tags=["mpmathew","docs"],
    default_args = {
    "owner": "mpmathew"
    },
    catchup=False,
):
    e1 = EmptyOperator(task_id="pre_dbt")

    # docs_tg = DbtDocsOperator(
    #     project_dir="/appz/home/airflow/dags/dbt/jaffle_shop",
    #     project_config=ProjectConfig(
    #     dbt_project_path=Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
    #     env_vars={"AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,"AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD},
    # ),
    #     profile_config=profile_config,
    #     execution_config=ExecutionConfig(
    #     dbt_executable_path="/dbt_venv/bin/dbt",
    # ),
    #     render_config=RenderConfig(
    #     load_method=LoadMode.DBT_LS,
    #     test_behavior=TestBehavior.NONE,
    # ),
    #     default_args={"retries": 2},
    #     task_id = "dbt_docs"
    # )
  
    generate_dbt_docs = DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir="/appz/home/airflow/dags/dbt/jaffle_shop",
        profile_config=profile_config,
        dbt_bin="/dbt_venv/bin/dbt",
        # docs-specific arguments
        # callback=upload_docs,
    )

    
    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> generate_dbt_docs >> e2

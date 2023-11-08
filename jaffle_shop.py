from pendulum import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig

from pathlib import Path


profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath="/appz/home/airflow/dags/dbt/jaffle_shop/profiles.yml",
)

#project_config = ProjectConfig(
#    dbt_project_path="/appz/home/airflow/dags/dbt/jaffle_shop",
#    models_relative_path="models",
#    seeds_relative_path="seeds",
#)

with DAG(
    dag_id="extract_dag",
    start_date=datetime(2023, 10, 24),
    schedule_interval="0 0 * 1 *",
):
    e1 = EmptyOperator(task_id="pre_dbt")

    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
    ),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        default_args={"retries": 2},
    )

    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> dbt_tg >> e2

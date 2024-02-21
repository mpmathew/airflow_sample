from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import TestBehavior
from pathlib import Path


profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath = "/appz/home/airflow/dags/dbt/jaffle_shop/profiles.yml",
)


with DAG(
    dag_id="jaffle_shop_demo",
    start_date=datetime(2023, 11, 10),
    schedule=None,
    tags=["mpmathew","demo"],
    default_args = {
    "owner": "mpmathew"
    },
    catchup=False,
):
    e1 = EmptyOperator(task_id="pre_dbt")

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
        test_behavior=TestBehavior.NONE,
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
        test_behavior=TestBehavior.NONE,
    ),
        default_args={"retries": 2},
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
        exclude=["path:models/staging","path:models/intermediate","path:seeds/"],
    ),
        default_args={"retries": 2},
    )
    
    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> seeds_tg >> stg_tg  >> dbt_tg >> e2

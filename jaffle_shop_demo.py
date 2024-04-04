import pendulum
from pendulum import Time, DateTime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import TestBehavior
from airflow.models import Variable
from pathlib import Path
from us_working_days_timetable import USWorkdayTimetable

# AIRFLOW_USER = "airflow"
# POSTGRES_TEST_PASSWORD = Variable.get("AIRFLOW_POSTGRES_TEST_PASSWORD")

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath = "/appz/home/airflow/dags/dbt/jaffle_shop/profiles.yml",
)

default_args = {
    "owner": "mpmathew",
}
with DAG(
    dag_id="jaffle_shop_demo",
    tags=["mpmathew","demo"],
    start_date=pendulum.datetime(2021, 1, 1, tz="America/Los_Angeles"),
    default_args=default_args,
    schedule=USWorkdayTimetable(Time(4:30)),
    catchup=False,
):
    e1 = EmptyOperator(task_id="pre_dbt")

    seeds_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        dbt_project_path=Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
        # env_vars={"AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,"AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD},
    ),
        # operator_args={
        #     "append_env": True,
        # },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=["path:seeds/"],
        test_behavior=TestBehavior.NONE,
    ),
        default_args={"retries": 2},
        group_id = "dbt_seeds_group"
    )

    stg_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        dbt_project_path=Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
        # env_vars={"AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,"AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD},
    ),
        # operator_args={
        #     "append_env": True,
        # },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=["path:models/staging/"],
        test_behavior=TestBehavior.NONE,
    ),
        default_args={"retries": 2},
        group_id = "dbt_stg_group"
    )

    dbt_tg = DbtTaskGroup(
        project_config=ProjectConfig(
        dbt_project_path=Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
        # env_vars={"AIRFLOW_POSTGRES_TEST_USER": AIRFLOW_USER,"AIRFLOW_POSTGRES_TEST_PASSWORD": POSTGRES_TEST_PASSWORD},
    ),
        # operator_args={
        #     "append_env": True,
        # },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        exclude=["path:models/staging","path:models/intermediate","path:seeds/"],
    ),
        default_args={"retries": 2},
    )
    
    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> seeds_tg >> stg_tg  >> dbt_tg >> e2

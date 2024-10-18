import pendulum
from pendulum import Time, DateTime, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import TestBehavior
from airflow.models import Variable
from pathlib import Path


## Variables defined
ENV_VAR_POSTGRES_USER = Variable.get("AIRFLOW_VAR_POSTGRES_USER", "test_user")
ENV_VAR_POSTGRES_PASSWORD = Variable.get("AIRFLOW_VAR_POSTGRES_PASSWORD")
DBT_VAR_STG_ORDR_NAME = "stg_orders"
DBT_VAR_ID_NAME = "customer_id"


## Profile config 
profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profiles_yml_filepath = "/appz/home/airflow/dags/dbt/jaffle_shop/profiles.yml",
)


## Project config 1 - without vars/env_vars
project_config_1 = ProjectConfig(
        dbt_project_path=Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
)


## Project config 2 - dbt_vars and env_vars defined
project_config_2 = ProjectConfig(
        dbt_project_path=Path("/appz/home/airflow/dags/dbt/jaffle_shop"),
        env_vars={"AIRFLOW_POSTGRES_USER": ENV_VAR_POSTGRES_USER,"AIRFLOW_POSTGRES_PASSWORD": ENV_VAR_POSTGRES_PASSWORD},
        dbt_vars={"staging_order_name": DBT_VAR_STG_ORDR_NAME,"id_name": DBT_VAR_ID_NAME},
)


## Default arguments
default_args = {
    "owner": "mpmathew",
}


with DAG(
    dag_id="JAFFLE_SHOP_ENV_VAR_DEMO",
    tags=["mpmathew","sample","var/env_var usage"],
    start_date=datetime(2023, 11, 10),
    default_args=default_args,
    schedule='0 0/12 * * *',
    catchup=False,
):
    e1 = EmptyOperator(task_id="pre_dbt")

    seeds_tg = DbtTaskGroup(
        project_config=project_config_1,
        operator_args={
            "append_env": True,
        },
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        # load_method=LoadMode.DBT_LS,
        select=["path:seeds/"],
        test_behavior=TestBehavior.NONE,
    ),
        default_args={"retries": 2},
        group_id = "dbt_seeds_group"
    )

    stg_tg = DbtTaskGroup(
        project_config=project_config_2,
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        # load_method=LoadMode.DBT_LS,
        select=["path:models/staging/"],
        test_behavior=TestBehavior.AFTER_ALL,
    ),
        default_args={"retries": 2},
        group_id = "dbt_stg_group"
    )

    dbt_tg = DbtTaskGroup(
        project_config=project_config_2,
        profile_config=profile_config,
        execution_config=ExecutionConfig(
        dbt_executable_path="/dbt_venv/bin/dbt",
    ),
        render_config=RenderConfig(
        # load_method=LoadMode.DBT_LS,
        exclude=["path:models/staging","path:models/intermediate","path:seeds/"],
    ),
        default_args={"retries": 2},
    )
    
    e2 = EmptyOperator(task_id="post_dbt")

    e1 >> seeds_tg >> stg_tg  >> dbt_tg >> e2
#test1

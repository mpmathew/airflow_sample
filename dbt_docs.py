from pendulum import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, RenderConfig, LoadMode
from cosmos.config import ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.constants import TestBehavior
from airflow.models import Variable
from pathlib import Path
from cosmos.operators import DbtDocsOperator

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
    
    generate_dbt_docs = DbtDocsOperator(
        task_id="generate_dbt_docs",
        project_dir="/appz/home/airflow/dags/dbt/jaffle_shop",
        profile_config=profile_config,
        # docs-specific arguments
        #callback=upload_docs,
)

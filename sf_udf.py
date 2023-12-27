from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from pendulum import datetime
from airflow.utils.task_group import TaskGroup

SNOWFLAKE_CONN_ID = "snowflake_connection"
SNOWFLAKE_SCHEMA = "TEST_DEV_DB.TEST_SCHEMA"

with DAG(
    "sf_udf",
    description="""
        Example DAG for udf, stored procedure and stream in Snowflake.
    """,
    doc_md=__doc__,
    start_date=datetime(2022, 12, 1),
    default_args={"snowflake_conn_id": SNOWFLAKE_CONN_ID},
    schedule=None,
    # defining the directory where SQL templates are stored
    template_searchpath="/appz/home/airflow/dags/dbt/jaffle_shop/objects/",
    catchup=False,
) as dag:

  create_udf = SnowflakeOperator(
        task_id="create_udf",
        sql="create_addone_udf.sql",
        params={"schema_name": SNOWFLAKE_SCHEMA},
    )
  create_sp = SnowflakeOperator(
        task_id="create_sp",
        sql="create_stored_procedure.sql",
        params={"schema_name": SNOWFLAKE_SCHEMA},
    )
  create_stream = SnowflakeOperator(
        task_id="create_stream",
        sql="create_stream.sql",
        params={"schema_name": SNOWFLAKE_SCHEMA},
    )
  begin = EmptyOperator(task_id="begin")
  end = EmptyOperator(task_id="end")

  chain(
      begin,
      create_udf,
      create_sp,
      create_stream,
      end,
 )

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_CONTAINER = "spark-iceberg"
PROJECT_PATH = "/home/iceberg/project"
SQLSERVER_JDBC_PACKAGE = "com.microsoft.sqlserver:mssql-jdbc:12.6.3.jre11"


def spark_submit_job(script_path: str, table_name: str | None = None) -> str:
    args = f"{script_path} {table_name}" if table_name else script_path

    return f"""
    docker exec {SPARK_CONTAINER} bash -c '
      cd {PROJECT_PATH} &&
      spark-submit \
        --packages {SQLSERVER_JDBC_PACKAGE} \
        {args}
    '
    """


with DAG(
    dag_id="init_salesorderdetail_snapshot",
    description="Runs the full snapshot load for SalesOrderDetail and initializes the CDC state.",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["init", "snapshot", "cdc", "salesorderdetail"],
) as dag:

    bronze_salesorderdetail_snapshot = BashOperator(
        task_id="bronze_salesorderdetail_snapshot",
        bash_command=spark_submit_job(
            "src/jobs/run_bronze.py",
            "saleslt_salesorderdetail_snapshot",
        ),
    )

    silver_salesorderdetail_snapshot = BashOperator(
        task_id="silver_salesorderdetail_snapshot",
        bash_command=spark_submit_job(
            "src/jobs/run_silver.py",
            "saleslt_salesorderdetail_snapshot",
        ),
    )

    init_cdc_state = BashOperator(
        task_id="init_cdc_state_salesorderdetail",
        bash_command=spark_submit_job(
            "src/jobs/init_cdc_state.py"
        ),
    )

    bronze_salesorderdetail_snapshot >> silver_salesorderdetail_snapshot >> init_cdc_state
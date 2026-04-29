from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


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
    dag_id="medallion_retail_pipeline",
    description="Orchestrates the PySpark Iceberg Medallion Retail pipeline.",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["pyspark", "iceberg", "medallion", "retail"],
) as dag:

    test_sqlserver_connection = BashOperator(
        task_id="test_sqlserver_connection",
        bash_command=spark_submit_job(
            "src/jobs/test_sqlserver_connection.py"
        ),
    )

    with TaskGroup("bronze_layer") as bronze_layer:

        bronze_customer = BashOperator(
            task_id="bronze_customer",
            bash_command=spark_submit_job(
                "src/jobs/run_bronze.py",
                "saleslt_customer",
            ),
        )

        bronze_address = BashOperator(
            task_id="bronze_address",
            bash_command=spark_submit_job(
                "src/jobs/run_bronze.py",
                "saleslt_address",
            ),
        )

        bronze_customeraddress = BashOperator(
            task_id="bronze_customeraddress",
            bash_command=spark_submit_job(
                "src/jobs/run_bronze.py",
                "saleslt_customeraddress",
            ),
        )

        bronze_product = BashOperator(
            task_id="bronze_product",
            bash_command=spark_submit_job(
                "src/jobs/run_bronze.py",
                "saleslt_product",
            ),
        )

        bronze_salesorderheader = BashOperator(
            task_id="bronze_salesorderheader",
            bash_command=spark_submit_job(
                "src/jobs/run_bronze.py",
                "saleslt_salesorderheader",
            ),
        )


        bronze_salesorderdetail_cdc = BashOperator(
            task_id="bronze_salesorderdetail_cdc",
            bash_command=spark_submit_job(
                "src/jobs/run_bronze.py",
                "saleslt_salesorderdetail_cdc",
            ),
        )

    with TaskGroup("silver_layer") as silver_layer:

        silver_customer = BashOperator(
            task_id="silver_customer",
            bash_command=spark_submit_job(
                "src/jobs/run_silver.py",
                "saleslt_customer",
            ),
        )

        silver_product = BashOperator(
            task_id="silver_product",
            bash_command=spark_submit_job(
                "src/jobs/run_silver.py",
                "saleslt_product",
            ),
        )

        silver_address = BashOperator(
            task_id="silver_address",
            bash_command=spark_submit_job(
                "src/jobs/run_silver.py",
                "saleslt_address",
            ),
        )

        silver_customeraddress = BashOperator(
            task_id="silver_customeraddress",
            bash_command=spark_submit_job(
                "src/jobs/run_silver.py",
                "saleslt_customeraddress",
            ),
        )

        silver_salesorderheader = BashOperator(
            task_id="silver_salesorderheader",
            bash_command=spark_submit_job(
                "src/jobs/run_silver.py",
                "saleslt_salesorderheader",
            ),
        )

        silver_salesorderdetail = BashOperator(
            task_id="silver_salesorderdetail",
            bash_command=spark_submit_job(
                "src/jobs/run_silver.py",
                "saleslt_salesorderdetail_cdc",
            ),
        )

    with TaskGroup("gold_layer") as gold_layer:

        gold_dim_customer = BashOperator(
            task_id="gold_dim_customer",
            bash_command=spark_submit_job(
                "src/jobs/run_gold.py",
                "dim_customer.sql"
            ),
        )
        gold_dim_product = BashOperator(
            task_id="gold_dim_product",
            bash_command=spark_submit_job(
                "src/jobs/run_gold.py",
                "dim_product.sql"
            ),
        )
        gold_fact_sales = BashOperator(
            task_id="gold_fact_sales",
            bash_command=spark_submit_job(
                "src/jobs/run_gold.py",
                "fact_sales.sql"
            ),
        )


    test_sqlserver_connection >> bronze_layer >> silver_layer >> gold_layer
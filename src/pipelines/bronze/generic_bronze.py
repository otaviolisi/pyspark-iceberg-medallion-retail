import uuid
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.spark_config import create_spark_session
from src.config.settings import (
    SQLSERVER_HOST,
    SQLSERVER_PORT,
    SQLSERVER_DATABASE,
    SQLSERVER_USER,
    SQLSERVER_PASSWORD,
    SQLSERVER_ENCRYPT,
    SQLSERVER_TRUST_SERVER_CERTIFICATE,
)
from src.config.tables_config import TABLES_CONFIG
from src.utils.watermark import read_watermark, write_watermark


def build_jdbc_url() -> str:
    return (
        f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT};"
        f"databaseName={SQLSERVER_DATABASE};"
        f"encrypt={SQLSERVER_ENCRYPT};"
        f"trustServerCertificate={SQLSERVER_TRUST_SERVER_CERTIFICATE};"
    )


def get_watermark_path(table_id: str) -> str:
    return f"state/{table_id}_bronze_watermark.json"


def get_table_config(table_id: str) -> dict[str, Any]:
    if table_id not in TABLES_CONFIG:
        available = ", ".join(TABLES_CONFIG.keys())
        raise ValueError(
            f"Table '{table_id}' not found in TABLES_CONFIG. Available tables: {available}"
        )
    return TABLES_CONFIG[table_id]


def ensure_bronze_namespace_exists(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")


def build_select_query(config: dict[str, Any], last_watermark: str | None) -> str:
    source_schema = config["source"]["schema"]
    source_table = config["source"]["table"]
    query_columns = config["source"]["query_columns"]
    load_strategy = config["load_strategy"]
    watermark_column = config["watermark_column"]

    selected_columns = ",\n                ".join(query_columns)

    if load_strategy in ("incremental_with_soft_delete", "incremental_upsert"):
        if not watermark_column:
            raise ValueError(
                f"Table {source_schema}.{source_table} requires watermark_column "
                f"for strategy {load_strategy}"
            )

        return f"""
        (
            SELECT
                {selected_columns}
            FROM {source_schema}.{source_table}
            WHERE {watermark_column} > CAST('{last_watermark}' AS DATETIME2)
        ) AS src
        """

    if load_strategy == "full_snapshot":
        return f"""
        (
            SELECT
                {selected_columns}
            FROM {source_schema}.{source_table}
        ) AS src
        """

    raise ValueError(f"Unsupported load_strategy: {load_strategy}")


def read_source_table(spark, jdbc_url: str, query: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("dbtable", query)
        .option("user", SQLSERVER_USER)
        .option("password", SQLSERVER_PASSWORD)
        .load()
    )


def add_bronze_metadata(df: DataFrame, table_id: str, config: dict[str, Any], last_watermark: str | None) -> DataFrame:
    source_schema = config["source"]["schema"]
    source_table = config["source"]["table"]
    load_strategy = config["load_strategy"]
    watermark_column = config["watermark_column"]
    soft_delete_column = config["soft_delete_column"]
    batch_id = str(uuid.uuid4())

    df_bronze = (
        df.withColumn("bronze_ingestion_timestamp", F.current_timestamp())
        .withColumn("bronze_batch_id", F.lit(batch_id))
        .withColumn("bronze_table_id", F.lit(table_id))
        .withColumn("bronze_source_system", F.lit("sqlserver"))
        .withColumn("bronze_source_schema", F.lit(source_schema))
        .withColumn("bronze_source_table", F.lit(source_table))
        .withColumn("bronze_load_strategy", F.lit(load_strategy))
        .withColumn("bronze_watermark_column", F.lit(watermark_column if watermark_column else ""))
        .withColumn("bronze_watermark_used", F.lit(last_watermark if last_watermark else ""))
        .withColumn("bronze_soft_delete_column", F.lit(soft_delete_column if soft_delete_column else ""))
    )

    return df_bronze


def create_bronze_table_if_not_exists(df_bronze: DataFrame, bronze_table: str) -> None:
    df_bronze.writeTo(bronze_table).create()


def append_to_bronze(df_bronze: DataFrame, bronze_table: str) -> None:
    df_bronze.writeTo(bronze_table).append()


def get_max_watermark(df: DataFrame, watermark_column: str) -> str | None:
    result = df.agg(F.max(F.col(watermark_column)).alias("max_watermark")).collect()[0]
    max_watermark = result["max_watermark"]
    return str(max_watermark) if max_watermark is not None else None


def run(table_id: str) -> None:
    config = get_table_config(table_id)
    load_strategy = config["load_strategy"]
    bronze_table = config["target"]["bronze_table"]
    watermark_column = config["watermark_column"]
    initial_watermark = config.get("initial_watermark")

    spark = create_spark_session(f"bronze-{table_id}")

    try:
        ensure_bronze_namespace_exists(spark)

        last_watermark = None
        watermark_path = None

        if load_strategy in ("incremental_with_soft_delete", "incremental_upsert"):
            watermark_path = get_watermark_path(table_id)
            last_watermark = read_watermark(
                file_path=watermark_path,
                default_value=initial_watermark,
            )

        jdbc_url = build_jdbc_url()
        query = build_select_query(config, last_watermark)

        print(f"Running bronze pipeline for table_id={table_id}")
        print(f"Strategy: {load_strategy}")
        print(f"Target bronze table: {bronze_table}")
        if last_watermark:
            print(f"Using watermark: {last_watermark}")

        df = read_source_table(spark, jdbc_url, query)

        print("Testing source read...")
        df.limit(5).show(truncate=False)

        sample_count = df.limit(1).count()

        if sample_count == 0:
            print("No records returned from source.")
            return

        df_bronze = add_bronze_metadata(
            df=df,
            table_id=table_id,
            config=config,
            last_watermark=last_watermark,
        )

        table_exists = spark.catalog.tableExists(bronze_table)

        if not table_exists:
            print(f"Bronze table does not exist. Creating: {bronze_table}")
            create_bronze_table_if_not_exists(df_bronze, bronze_table)
        else:
            print(f"Appending data to bronze table: {bronze_table}")
            append_to_bronze(df_bronze, bronze_table)

        if load_strategy in ("incremental_with_soft_delete", "incremental_upsert"):
            max_watermark = get_max_watermark(df, watermark_column)

            if max_watermark is not None and watermark_path is not None:
                write_watermark(watermark_path, max_watermark)
                print(f"Updated watermark saved: {max_watermark}")

        row_count = df_bronze.count()
        print(f"Bronze load completed successfully. Rows processed: {row_count}")

    finally:
        spark.stop()
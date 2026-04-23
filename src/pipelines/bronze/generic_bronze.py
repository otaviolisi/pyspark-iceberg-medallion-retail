import uuid
from datetime import date, datetime
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


def get_cdc_lsn_path(table_id: str) -> str:
    return f"state/{table_id}_bronze_lsn.json"


def get_table_config(table_id: str) -> dict[str, Any]:
    if table_id not in TABLES_CONFIG:
        available = ", ".join(TABLES_CONFIG.keys())
        raise ValueError(
            f"Table '{table_id}' not found in TABLES_CONFIG. Available tables: {available}"
        )
    return TABLES_CONFIG[table_id]


def ensure_bronze_namespace_exists(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")


def read_single_value(spark, jdbc_url: str, query: str, column_name: str) -> str | None:
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .option("dbtable", f"({query}) AS q")
        .option("user", SQLSERVER_USER)
        .option("password", SQLSERVER_PASSWORD)
        .load()
    )

    rows = df.collect()
    if not rows:
        return None

    return rows[0][column_name]


def get_cdc_max_lsn(spark, jdbc_url: str) -> str:
    query = """
        SELECT CONVERT(varchar(42), sys.fn_cdc_get_max_lsn(), 1) AS max_lsn
    """
    result = read_single_value(spark, jdbc_url, query, "max_lsn")
    if not result:
        raise ValueError("Could not retrieve current CDC max LSN.")
    return result


def get_cdc_min_lsn(spark, jdbc_url: str, capture_instance: str) -> str:
    query = f"""
        SELECT CONVERT(varchar(42), sys.fn_cdc_get_min_lsn('{capture_instance}'), 1) AS min_lsn
    """
    result = read_single_value(spark, jdbc_url, query, "min_lsn")
    if not result:
        raise ValueError(f"Could not retrieve CDC min LSN for capture instance {capture_instance}.")
    return result


def build_select_query(
    config: dict[str, Any],
    last_watermark: str | None = None,
    cdc_from_lsn: str | None = None,
    cdc_to_lsn: str | None = None,
) -> str:
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

        if not last_watermark:
            raise ValueError(
                f"last_watermark is required for strategy {load_strategy}"
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

    if load_strategy == "incremental_cdc":
        cdc_config = config.get("cdc", {})
        capture_instance = cdc_config.get("capture_instance")
        row_filter_option = cdc_config.get("row_filter_option", "all")

        if not capture_instance:
            raise ValueError("CDC config requires 'capture_instance'.")

        if not cdc_from_lsn or not cdc_to_lsn:
            raise ValueError("CDC query requires from_lsn and to_lsn.")

        return f"""
        (
            SELECT
                {selected_columns}
            FROM cdc.fn_cdc_get_all_changes_{capture_instance}(
                sys.fn_cdc_increment_lsn(CONVERT(binary(10), '{cdc_from_lsn}', 1)),
                CONVERT(binary(10), '{cdc_to_lsn}', 1),
                '{row_filter_option}'
            )
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


def add_bronze_metadata(
    df: DataFrame,
    table_id: str,
    config: dict[str, Any],
    last_watermark: str | None,
    cdc_from_lsn: str | None = None,
    cdc_to_lsn: str | None = None,
) -> DataFrame:
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
        .withColumn(
            "bronze_watermark_column",
            F.lit(watermark_column if watermark_column else ""),
        )
        .withColumn(
            "bronze_watermark_used",
            F.lit(last_watermark if last_watermark else ""),
        )
        .withColumn(
            "bronze_soft_delete_column",
            F.lit(soft_delete_column if soft_delete_column else ""),
        )
        .withColumn("bronze_cdc_from_lsn", F.lit(cdc_from_lsn if cdc_from_lsn else ""))
        .withColumn("bronze_cdc_to_lsn", F.lit(cdc_to_lsn if cdc_to_lsn else ""))
    )

    return df_bronze


def create_bronze_table_if_not_exists(df_bronze: DataFrame, bronze_table: str) -> None:
    df_bronze.writeTo(bronze_table).create()


def append_to_bronze(df_bronze: DataFrame, bronze_table: str) -> None:
    df_bronze.writeTo(bronze_table).append()


def replace_bronze_table(spark, df_bronze: DataFrame, bronze_table: str) -> None:
    if spark.catalog.tableExists(bronze_table):
        spark.sql(f"DROP TABLE {bronze_table}")
    df_bronze.writeTo(bronze_table).create()


def format_watermark_value(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    return str(value)


def get_max_watermark(df: DataFrame, watermark_column: str) -> str | None:
    result = df.agg(F.max(F.col(watermark_column)).alias("max_watermark")).collect()[0]
    max_watermark = result["max_watermark"]
    return format_watermark_value(max_watermark)


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
        cdc_from_lsn = None
        cdc_to_lsn = None
        cdc_lsn_path = None

        jdbc_url = build_jdbc_url()

        if load_strategy in ("incremental_with_soft_delete", "incremental_upsert"):
            watermark_path = get_watermark_path(table_id)
            last_watermark = read_watermark(
                file_path=watermark_path,
                default_value=initial_watermark,
            )

        if load_strategy == "incremental_cdc":
            cdc_lsn_path = get_cdc_lsn_path(table_id)
            cdc_from_lsn = read_watermark(file_path=cdc_lsn_path, default_value=None)

            if not cdc_from_lsn:
                raise ValueError(
                    f"CDC state not initialized for table_id={table_id}. "
                    f"Run the initial snapshot first and then initialize the CDC LSN state."
                )

            cdc_to_lsn = get_cdc_max_lsn(spark, jdbc_url)

            if cdc_from_lsn == cdc_to_lsn:
                print("No new CDC changes to process.")
                return

        query = build_select_query(
            config=config,
            last_watermark=last_watermark,
            cdc_from_lsn=cdc_from_lsn,
            cdc_to_lsn=cdc_to_lsn,
        )

        print(f"Running bronze pipeline for table_id={table_id}")
        print(f"Strategy: {load_strategy}")
        print(f"Target bronze table: {bronze_table}")

        if last_watermark:
            print(f"Using watermark: {last_watermark}")

        if cdc_from_lsn and cdc_to_lsn:
            print(f"CDC window from {cdc_from_lsn} to {cdc_to_lsn}")

        df = read_source_table(spark, jdbc_url, query)

        print("Testing source read...")
        df.limit(5).show(truncate=False)

        sample_count = df.limit(1).count()

        if sample_count == 0:
            print("No records returned from source.")
            if load_strategy == "incremental_cdc" and cdc_to_lsn and cdc_lsn_path:
                write_watermark(cdc_lsn_path, cdc_to_lsn)
                print(f"CDC LSN state advanced to: {cdc_to_lsn}")
            return

        df_bronze = add_bronze_metadata(
            df=df,
            table_id=table_id,
            config=config,
            last_watermark=last_watermark,
            cdc_from_lsn=cdc_from_lsn,
            cdc_to_lsn=cdc_to_lsn,
        )

        table_exists = spark.catalog.tableExists(bronze_table)

        if load_strategy == "full_snapshot":
            print(f"Replacing bronze table with full snapshot: {bronze_table}")
            replace_bronze_table(spark, df_bronze, bronze_table)
        else:
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

        if load_strategy == "incremental_cdc":
            if cdc_to_lsn is not None and cdc_lsn_path is not None:
                write_watermark(cdc_lsn_path, cdc_to_lsn)
                print(f"Updated CDC LSN saved: {cdc_to_lsn}")

        row_count = df_bronze.count()
        print(f"Bronze load completed successfully. Rows processed: {row_count}")

    finally:
        spark.stop()
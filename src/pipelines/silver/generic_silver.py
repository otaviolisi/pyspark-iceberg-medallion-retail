from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.config.spark_config import create_spark_session
from src.config.tables_config import TABLES_CONFIG


def get_table_config(table_id: str) -> dict[str, Any]:
    if table_id not in TABLES_CONFIG:
        available = ", ".join(TABLES_CONFIG.keys())
        raise ValueError(
            f"Table '{table_id}' not found in TABLES_CONFIG. Available tables: {available}"
        )
    return TABLES_CONFIG[table_id]


def ensure_silver_namespace_exists(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.silver")

def q(col_name: str) -> str:
    return f"`{col_name}`"    


def read_table(spark, table_name: str) -> DataFrame:
    return spark.table(table_name)


def table_exists(spark, table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def replace_table(spark, df: DataFrame, table_name: str) -> None:
    if spark.catalog.tableExists(table_name):
        spark.sql(f"DROP TABLE {table_name}")
    df.writeTo(table_name).create()


def append_table(spark, df: DataFrame, table_name: str) -> None:
    if df.limit(1).count() == 0:
        return

    if not spark.catalog.tableExists(table_name):
        df.writeTo(table_name).create()
    else:
        df.writeTo(table_name).append()


def get_latest_bronze_batch_df(spark, bronze_table: str) -> DataFrame:
    df = read_table(spark, bronze_table)

    latest_batch_ts = (
        df.agg(F.max("bronze_ingestion_timestamp").alias("max_ts"))
        .collect()[0]["max_ts"]
    )

    return df.filter(F.col("bronze_ingestion_timestamp") == latest_batch_ts)


def build_merge_condition(
    primary_key: list[str],
    source_alias: str = "s",
    target_alias: str = "t",
) -> str:
    return " AND ".join(
        [f"{target_alias}.{col} = {source_alias}.{col}" for col in primary_key]
    )
def drop_cdc_technical_columns(df: DataFrame) -> DataFrame:
    cdc_technical_columns = [
        "__$start_lsn",
        "__$seqval",
        "__$operation",
        "__$update_mask",
    ]

    columns_to_drop = [col for col in cdc_technical_columns if col in df.columns]

    return df.drop(*columns_to_drop)

def align_source_to_target_columns(
    spark,
    df_source: DataFrame,
    target_table: str,
) -> DataFrame:
    target_columns = spark.table(target_table).columns

    selected_columns = [
        col for col in target_columns
        if col in df_source.columns
    ]

    return df_source.select(*selected_columns)

def prepare_soft_delete_incremental_df(df: DataFrame, config: dict[str, Any]) -> DataFrame:
    soft_delete_column = config["soft_delete_column"]

    if soft_delete_column:
        return df.withColumn(
            "is_deleted",
            F.when(F.col(soft_delete_column).isNotNull(), F.lit(1)).otherwise(F.lit(0)),
        )

    return df.withColumn("is_deleted", F.lit(0))


def merge_incremental_soft_delete(
    spark,
    df_source: DataFrame,
    silver_table: str,
    primary_key: list[str],
) -> None:
    if not table_exists(spark, silver_table):
        df_source.writeTo(silver_table).create()
        return

    df_source.createOrReplaceTempView("silver_source_tmp")

    merge_condition = build_merge_condition(primary_key)

    update_assignments = ",\n            ".join(
        [f"t.{col} = s.{col}" for col in df_source.columns]
    )

    insert_columns = ", ".join(df_source.columns)
    insert_values = ", ".join([f"s.{col}" for col in df_source.columns])

    sql = f"""
    MERGE INTO {silver_table} t
    USING silver_source_tmp s
      ON {merge_condition}
    WHEN MATCHED THEN
      UPDATE SET
        {update_assignments}
    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """

    spark.sql(sql)


def prepare_incremental_upsert_df(df_bronze: DataFrame, config: dict[str, Any]) -> DataFrame:
    watermark_column = config["watermark_column"]
    primary_key = config["primary_key"]

    window_spec = Window.partitionBy(*primary_key).orderBy(F.col(watermark_column).desc())

    return (
        df_bronze
        .withColumn("silver_rank", F.row_number().over(window_spec))
        .withColumn(
            "is_current",
            F.when(F.col("silver_rank") == 1, F.lit(1)).otherwise(F.lit(0)),
        )
        .drop("silver_rank")
    )


def rebuild_incremental_upsert_current_flag(
    spark,
    silver_table: str,
    config: dict[str, Any],
) -> None:
    df = read_table(spark, silver_table)
    watermark_column = config["watermark_column"]
    primary_key = config["primary_key"]

    window_spec = Window.partitionBy(*primary_key).orderBy(F.col(watermark_column).desc())

    df_rebuilt = (
        df
        .withColumn("silver_rank", F.row_number().over(window_spec))
        .withColumn(
            "is_current",
            F.when(F.col("silver_rank") == 1, F.lit(1)).otherwise(F.lit(0)),
        )
        .drop("silver_rank")
    )

    replace_table(spark, df_rebuilt, silver_table)


def process_incremental_upsert(
    spark,
    df_bronze: DataFrame,
    silver_table: str,
    config: dict[str, Any],
) -> None:
    df_prepared = prepare_incremental_upsert_df(df_bronze, config)
    append_table(spark, df_prepared, silver_table)
    rebuild_incremental_upsert_current_flag(spark, silver_table, config)


def process_full_snapshot(spark, bronze_table: str, silver_table: str) -> None:
    df_bronze = get_latest_bronze_batch_df(spark, bronze_table)

    if "is_deleted" not in df_bronze.columns:
        df_bronze = df_bronze.withColumn("is_deleted", F.lit(0))

    replace_table(spark, df_bronze, silver_table)


def filter_relevant_cdc_operations(df_cdc: DataFrame) -> DataFrame:
    return df_cdc.filter(F.col("__$operation").isin([1, 2, 4]))


def get_latest_cdc_rows(df_cdc: DataFrame, primary_key: list[str]) -> DataFrame:
    window_spec = Window.partitionBy(*primary_key).orderBy(
        F.col("__$start_lsn").desc(),
        F.col("__$seqval").desc()
    )

    return (
        df_cdc
        .withColumn("cdc_rank", F.row_number().over(window_spec))
        .filter(F.col("cdc_rank") == 1)
        .drop("cdc_rank")
    )


def prepare_cdc_for_merge(df_cdc: DataFrame) -> DataFrame:
    return (
        df_cdc
        .withColumn(
            "is_deleted",
            F.when(F.col("__$operation") == 1, F.lit(1)).otherwise(F.lit(0)),
        )
    )


def merge_incremental_cdc(
    spark,
    df_cdc: DataFrame,
    silver_table: str,
    primary_key: list[str],
) -> None:
    if not table_exists(spark, silver_table):
        raise ValueError(
            f"Silver table {silver_table} does not exist. "
            "Run the snapshot silver load first."
        )

    df_cdc.createOrReplaceTempView("silver_cdc_tmp")

    merge_condition = build_merge_condition(primary_key)

    update_columns = [
        col for col in df_cdc.columns
        if col not in primary_key and col != "is_deleted"
    ]

    update_assignments = ",\n            ".join(
        [f"t.`{col}` = s.`{col}`" for col in update_columns]
        + ["t.is_deleted = s.is_deleted"]
    )

    insert_columns = ", ".join([f"`{col}`" for col in df_cdc.columns])
    insert_values = ", ".join([f"s.`{col}`" for col in df_cdc.columns])

    sql = f"""
    MERGE INTO {silver_table} t
    USING silver_cdc_tmp s
      ON {merge_condition}

    WHEN MATCHED AND s.is_deleted = 1 THEN
      UPDATE SET
        t.is_deleted = 1

    WHEN MATCHED AND s.is_deleted = 0 THEN
      UPDATE SET
        {update_assignments}

    WHEN NOT MATCHED THEN
      INSERT ({insert_columns})
      VALUES ({insert_values})
    """

    spark.sql(sql)


def run(table_id: str) -> None:
    config = get_table_config(table_id)
    load_strategy = config["load_strategy"]
    bronze_table = config["target"]["bronze_table"]
    silver_table = config["target"]["silver_table"]
    primary_key = config["primary_key"]

    spark = create_spark_session(f"silver-{table_id}")

    try:
        ensure_silver_namespace_exists(spark)

        if load_strategy == "full_snapshot":
            print(f"Running silver full snapshot for {table_id}")
            process_full_snapshot(spark, bronze_table, silver_table)
            print("Silver full snapshot completed successfully.")
            return

        df_bronze = get_latest_bronze_batch_df(spark, bronze_table)

        if df_bronze.limit(1).count() == 0:
            print("No records found in bronze for latest batch.")
            return

        if load_strategy == "incremental_with_soft_delete":
            print(f"Running silver merge for soft delete strategy: {table_id}")
            df_source = prepare_soft_delete_incremental_df(df_bronze, config)
            merge_incremental_soft_delete(spark, df_source, silver_table, primary_key)
            print("Silver merge completed successfully.")
            return

        if load_strategy == "incremental_upsert":
            print(f"Running silver append versioned load for: {table_id}")
            process_incremental_upsert(spark, df_bronze, silver_table, config)
            print("Silver incremental upsert completed successfully.")
            return

        if load_strategy == "incremental_cdc":
            print(f"Running silver CDC merge for: {table_id}")

            df_cdc_filtered = filter_relevant_cdc_operations(df_bronze)
            if df_cdc_filtered.limit(1).count() == 0:
                print("No relevant CDC operations found in latest batch.")
                return

            df_cdc_latest = get_latest_cdc_rows(df_cdc_filtered, primary_key)
            df_cdc_prepared = prepare_cdc_for_merge(df_cdc_latest)
            df_cdc_prepared = drop_cdc_technical_columns(df_cdc_prepared)
            df_cdc_prepared = align_source_to_target_columns(
                spark=spark,
                df_source=df_cdc_prepared,
                target_table=silver_table,
            )

            merge_incremental_cdc(spark, df_cdc_prepared, silver_table, primary_key)  
            print("Silver CDC merge completed successfully.")
            return

        raise ValueError(f"Unsupported silver load_strategy: {load_strategy}")

    finally:
        spark.stop()
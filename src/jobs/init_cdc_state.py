import sys

from src.config.spark_config import create_spark_session
from src.pipelines.bronze.generic_bronze import (
    build_jdbc_url,
    get_cdc_lsn_path,
    get_cdc_max_lsn,
    get_table_config,
)
from src.utils.watermark import write_watermark


def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: spark-submit src/jobs/init_cdc_state.py <table_id>")

    table_id = sys.argv[1]
    config = get_table_config(table_id)

    if config["load_strategy"] != "incremental_cdc":
        raise ValueError(f"Table {table_id} is not configured as incremental_cdc")

    spark = create_spark_session(f"init-cdc-state-{table_id}")

    try:
        jdbc_url = build_jdbc_url()
        current_lsn = get_cdc_max_lsn(spark, jdbc_url)
        state_path = get_cdc_lsn_path(table_id)

        write_watermark(state_path, current_lsn)
        print(f"CDC state initialized for {table_id}: {current_lsn}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
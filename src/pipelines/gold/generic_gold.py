from pathlib import Path

from src.config.spark_config import create_spark_session


def run(sql_file: str) -> None:
    spark = create_spark_session(f"gold-{sql_file}")

    try:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.gold")

        sql_path = Path("src/pipelines/gold/sql") / sql_file
        query = sql_path.read_text()

        spark.sql(query)

        print(f"Gold script executed successfully: {sql_file}")

    finally:
        spark.stop()
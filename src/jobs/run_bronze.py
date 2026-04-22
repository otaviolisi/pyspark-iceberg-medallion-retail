import sys

from src.pipelines.bronze.generic_bronze import run


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("You must inform table_id. Example: uv run python -m src.jobs.run_bronze saleslt_product")

    table_id = sys.argv[1]
    run(table_id)
    
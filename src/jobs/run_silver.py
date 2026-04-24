import sys

from src.pipelines.silver.generic_silver import run


def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: spark-submit src/jobs/run_silver.py <table_id>")

    table_id = sys.argv[1]
    run(table_id)


if __name__ == "__main__":
    main()
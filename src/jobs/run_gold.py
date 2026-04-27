import sys

from src.pipelines.gold.generic_gold import run


def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: spark-submit src/jobs/run_gold.py <sql_file>")

    run(sys.argv[1])


if __name__ == "__main__":
    main()
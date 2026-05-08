from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="gharchive_raw_to_bronze",
    description="Convert GH Archive raw JSON.gz files to Bronze Parquet using Spark",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gharchive", "raw", "bronze", "spark"],
) as dag:

    validate_raw_files = BashOperator(
        task_id="validate_raw_files",
        bash_command="""
        echo "Checking raw GH Archive files..."
        ls -lh /opt/data-platform/data/raw/gharchive/year=2025/month=01/day=01/

        FILE_COUNT=$(ls /opt/data-platform/data/raw/gharchive/year=2025/month=01/day=01/*.json.gz | wc -l)
        echo "Raw file count: ${FILE_COUNT}"

        test "${FILE_COUNT}" -eq 24
        """,
    )

    raw_to_bronze_spark = BashOperator(
        task_id="raw_to_bronze_spark",
        bash_command="""
        echo "Running Spark raw to bronze job..."
        python /opt/data-platform/scripts/raw_to_bronze_spark.py
        """,
    )

    validate_bronze_output = BashOperator(
        task_id="validate_bronze_output",
        bash_command="""
        echo "Checking Bronze output..."
        ls -lh /opt/data-platform/data/bronze/gharchive/events/year=2025/month=01/day=01/

        test -f /opt/data-platform/data/bronze/gharchive/events/year=2025/month=01/day=01/_SUCCESS
        """,
    )

    validate_raw_files >> raw_to_bronze_spark >> validate_bronze_output
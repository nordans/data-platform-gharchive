from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="gharchive_bronze_to_silver",
    description="Convert GH Archive Bronze Parquet to Silver Parquet using Spark",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gharchive", "bronze", "silver", "spark"],
) as dag:

    validate_bronze_input = BashOperator(
        task_id="validate_bronze_input",
        bash_command="""
        echo "Checking Bronze input..."
        ls -lh /opt/data-platform/data/bronze/gharchive/events/year=2025/month=01/day=01/

        test -f /opt/data-platform/data/bronze/gharchive/events/year=2025/month=01/day=01/_SUCCESS
        """,
    )

    bronze_to_silver_spark = BashOperator(
        task_id="bronze_to_silver_spark",
        bash_command="""
        echo "Running Spark bronze to silver job..."
        python /opt/data-platform/scripts/bronze_to_silver_spark.py
        """,
    )

    validate_silver_output = BashOperator(
        task_id="validate_silver_output",
        bash_command="""
        echo "Checking Silver output..."
        ls -lh /opt/data-platform/data/silver/gharchive/events/year=2025/month=01/day=01/

        test -f /opt/data-platform/data/silver/gharchive/events/year=2025/month=01/day=01/_SUCCESS
        """,
    )

    record_pipeline_metrics = BashOperator(
        task_id="record_pipeline_metrics",
        bash_command="""
        echo "Recording bronze_to_silver metrics..."

        BRONZE_PATH="/opt/data-platform/data/bronze/gharchive/events/year=2025/month=01/day=01"
        SILVER_PATH="/opt/data-platform/data/silver/gharchive/events/year=2025/month=01/day=01"

        ROWS_INPUT=$(python /opt/data-platform/scripts/count_parquet_rows.py \
        --paths "${BRONZE_PATH}" \
        | tail -n 1)

        ROWS_OUTPUT=$(python /opt/data-platform/scripts/count_parquet_rows.py \
        --paths "${SILVER_PATH}" \
        | tail -n 1)

        ROWS_REJECTED=$((ROWS_INPUT - ROWS_OUTPUT))

        python /opt/data-platform/scripts/pipeline_metrics.py \
        --pipeline-name "gharchive_bronze_to_silver" \
        --layer-from "bronze" \
        --layer-to "silver" \
        --status "success" \
        --data-date "2025-01-01" \
        --rows-input "${ROWS_INPUT}" \
        --rows-output "${ROWS_OUTPUT}" \
        --rows-rejected "${ROWS_REJECTED}" \
        --dataset-count "1"
        """,
    )

    validate_bronze_input >> bronze_to_silver_spark >> validate_silver_output >> record_pipeline_metrics
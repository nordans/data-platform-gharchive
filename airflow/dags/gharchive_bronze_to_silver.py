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

    validate_bronze_input >> bronze_to_silver_spark >> validate_silver_output
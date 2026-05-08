from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="gharchive_silver_to_gold",
    description="Create GH Archive Gold datasets for BI, analytics and ML from Silver Parquet",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gharchive", "silver", "gold", "spark", "bi", "analytics", "ml"],
) as dag:

    validate_silver_input = BashOperator(
        task_id="validate_silver_input",
        bash_command="""
        echo "Checking Silver input..."
        ls -lh /opt/data-platform/data/silver/gharchive/events/year=2025/month=01/day=01/

        test -f /opt/data-platform/data/silver/gharchive/events/year=2025/month=01/day=01/_SUCCESS
        """,
    )

    silver_to_gold_spark = BashOperator(
        task_id="silver_to_gold_spark",
        bash_command="""
        echo "Running Spark silver to gold job..."
        python /opt/data-platform/scripts/silver_to_gold_spark.py
        """,
    )

    validate_gold_output = BashOperator(
        task_id="validate_gold_output",
        bash_command="""
        echo "Checking Gold output..."

        test -f /opt/data-platform/data/gold/gharchive/event_type_hourly/year=2025/month=01/day=01/_SUCCESS
        test -f /opt/data-platform/data/gold/gharchive/top_repositories_daily/year=2025/month=01/day=01/_SUCCESS
        test -f /opt/data-platform/data/gold/gharchive/repo_activity_features/year=2025/month=01/day=01/_SUCCESS

        echo "Gold BI:"
        ls -lh /opt/data-platform/data/gold/gharchive/event_type_hourly/year=2025/month=01/day=01/

        echo "Gold analytics:"
        ls -lh /opt/data-platform/data/gold/gharchive/top_repositories_daily/year=2025/month=01/day=01/

        echo "Gold ML:"
        ls -lh /opt/data-platform/data/gold/gharchive/repo_activity_features/year=2025/month=01/day=01/
        """,
    )

    validate_silver_input >> silver_to_gold_spark >> validate_gold_output
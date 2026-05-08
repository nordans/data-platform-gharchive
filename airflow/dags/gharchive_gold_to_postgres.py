from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="gharchive_gold_to_postgres",
    description="Load GH Archive Gold Parquet datasets into PostgreSQL serving tables",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gharchive", "gold", "postgres", "serving", "bi"],
) as dag:

    validate_gold_input = BashOperator(
        task_id="validate_gold_input",
        bash_command="""
        echo "Checking Gold input..."

        test -f /opt/data-platform/data/gold/gharchive/event_type_hourly/year=2025/month=01/day=01/_SUCCESS
        test -f /opt/data-platform/data/gold/gharchive/top_repositories_daily/year=2025/month=01/day=01/_SUCCESS
        test -f /opt/data-platform/data/gold/gharchive/repo_activity_features/year=2025/month=01/day=01/_SUCCESS

        echo "Gold input exists."
        """,
    )

    load_gold_to_postgres = BashOperator(
        task_id="load_gold_to_postgres",
        bash_command="""
        echo "Loading Gold datasets to PostgreSQL..."
        python /opt/data-platform/scripts/gold_to_postgres.py
        """,
    )

    validate_postgres_tables = BashOperator(
        task_id="validate_postgres_tables",
        bash_command="""
        python - <<'PY'
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://data_user:data_pass@postgres:5432/data_platform")

tables = [
    "gold.event_type_hourly",
    "gold.top_repositories_daily",
    "gold.repo_activity_features",
]

with engine.connect() as conn:
    for table in tables:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        print(f"{table}: {count} rows")
        if count == 0:
            raise RuntimeError(f"{table} is empty")
PY
        """,
    )

    record_pipeline_metrics = BashOperator(
        task_id="record_pipeline_metrics",
        bash_command="""
        echo "Recording gold_to_postgres metrics..."

        GOLD_EVENT_TYPE_PATH="/opt/data-platform/data/gold/gharchive/event_type_hourly/year=2025/month=01/day=01"
        GOLD_TOP_REPOS_PATH="/opt/data-platform/data/gold/gharchive/top_repositories_daily/year=2025/month=01/day=01"
        GOLD_REPO_FEATURES_PATH="/opt/data-platform/data/gold/gharchive/repo_activity_features/year=2025/month=01/day=01"

        ROWS_INPUT=$(python /opt/data-platform/scripts/count_parquet_rows.py \
        --paths \
            "${GOLD_EVENT_TYPE_PATH}" \
            "${GOLD_TOP_REPOS_PATH}" \
            "${GOLD_REPO_FEATURES_PATH}" \
        | tail -n 1)

        ROWS_OUTPUT=$(python /opt/data-platform/scripts/count_postgres_rows.py \
        --tables \
            gold.event_type_hourly \
            gold.top_repositories_daily \
            gold.repo_activity_features \
        | tail -n 1)

        ROWS_REJECTED=$((ROWS_INPUT - ROWS_OUTPUT))

        python /opt/data-platform/scripts/pipeline_metrics.py \
        --pipeline-name "gharchive_gold_to_postgres" \
        --layer-from "gold" \
        --layer-to "postgres" \
        --status "success" \
        --data-date "2025-01-01" \
        --rows-input "${ROWS_INPUT}" \
        --rows-output "${ROWS_OUTPUT}" \
        --rows-rejected "0" \
        --dataset-count "3"
        """,
    )  

    validate_gold_input >> load_gold_to_postgres >> validate_postgres_tables >> record_pipeline_metrics
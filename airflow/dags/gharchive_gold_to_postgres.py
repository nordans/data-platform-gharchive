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

    validate_gold_input >> load_gold_to_postgres >> validate_postgres_tables
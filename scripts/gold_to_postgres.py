from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


POSTGRES_URL = "postgresql+psycopg2://data_user:data_pass@postgres:5432/data_platform"

GOLD_ROOT = Path("/opt/data-platform/data/gold/gharchive")

EVENT_TYPE_HOURLY_PATH = GOLD_ROOT / "event_type_hourly/year=2025/month=01/day=01"
TOP_REPOSITORIES_PATH = GOLD_ROOT / "top_repositories_daily/year=2025/month=01/day=01"
REPO_FEATURES_PATH = GOLD_ROOT / "repo_activity_features/year=2025/month=01/day=01"


def read_parquet_dir(path: Path) -> pd.DataFrame:
    files = list(path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {path}")

    return pd.read_parquet(path)


def main() -> None:
    engine = create_engine(POSTGRES_URL)

    print("Reading Gold BI dataset...")
    event_type_hourly = read_parquet_dir(EVENT_TYPE_HOURLY_PATH)

    print("Reading Gold analytics dataset...")
    top_repositories = read_parquet_dir(TOP_REPOSITORIES_PATH)

    # Do BI nie potrzebujemy ładować milionów repozytoriów.
    # Bierzemy top 5000, żeby dashboard był szybki.
    top_repositories = (
        top_repositories
        .sort_values("total_events", ascending=False)
        .head(5000)
    )

    print("Reading Gold ML features dataset...")
    repo_features = read_parquet_dir(REPO_FEATURES_PATH)

    # Do podglądu ML w Metabase też ograniczamy.
    repo_features = (
        repo_features
        .sort_values("total_events", ascending=False)
        .head(5000)
    )

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))

    print("Writing gold.event_type_hourly...")
    event_type_hourly.to_sql(
        "event_type_hourly",
        engine,
        schema="gold",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    print("Writing gold.top_repositories_daily...")
    top_repositories.to_sql(
        "top_repositories_daily",
        engine,
        schema="gold",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    print("Writing gold.repo_activity_features...")
    repo_features.to_sql(
        "repo_activity_features",
        engine,
        schema="gold",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_event_type_hourly_hour
            ON gold.event_type_hourly(event_date, event_hour);
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_top_repositories_events
            ON gold.top_repositories_daily(total_events DESC);
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_repo_features_activity
            ON gold.repo_activity_features(total_events DESC);
        """))

    print("Gold datasets loaded to PostgreSQL successfully.")


if __name__ == "__main__":
    main()
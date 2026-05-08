from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import create_engine, text


POSTGRES_URL = "postgresql+psycopg2://data_user:data_pass@postgres:5432/data_platform"


def parse_optional_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    return int(value)


def parse_optional_float(value: Optional[str]) -> Optional[float]:
    if value is None or value == "":
        return None
    return float(value)


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--layer-from", required=True)
    parser.add_argument("--layer-to", required=True)
    parser.add_argument("--status", required=True)

    parser.add_argument("--data-date", default=None)
    parser.add_argument("--rows-input", default=None)
    parser.add_argument("--rows-output", default=None)
    parser.add_argument("--rows-rejected", default=None)
    parser.add_argument("--dataset-count", default=None)
    parser.add_argument("--duration-seconds", default=None)
    parser.add_argument("--error-message", default=None)

    args = parser.parse_args()

    engine = create_engine(POSTGRES_URL)
    now = datetime.now(timezone.utc)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS ops"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ops.pipeline_run_metrics (
                id BIGSERIAL PRIMARY KEY,
                pipeline_name TEXT NOT NULL,
                layer_from TEXT NOT NULL,
                layer_to TEXT NOT NULL,
                status TEXT NOT NULL,
                data_date DATE NULL,
                rows_input BIGINT NULL,
                rows_output BIGINT NULL,
                rows_rejected BIGINT NULL,
                dataset_count BIGINT NULL,
                duration_seconds NUMERIC NULL,
                error_message TEXT NULL,
                recorded_at TIMESTAMPTZ NOT NULL
            )
        """))

        # Dla istniejącej starej tabeli — dodaje kolumny, jeśli ich brakuje.
        conn.execute(text("ALTER TABLE ops.pipeline_run_metrics ADD COLUMN IF NOT EXISTS data_date DATE NULL"))
        conn.execute(text("ALTER TABLE ops.pipeline_run_metrics ADD COLUMN IF NOT EXISTS rows_rejected BIGINT NULL"))
        conn.execute(text("ALTER TABLE ops.pipeline_run_metrics ADD COLUMN IF NOT EXISTS dataset_count BIGINT NULL"))

        conn.execute(
            text("""
                INSERT INTO ops.pipeline_run_metrics (
                    pipeline_name,
                    layer_from,
                    layer_to,
                    status,
                    data_date,
                    rows_input,
                    rows_output,
                    rows_rejected,
                    dataset_count,
                    duration_seconds,
                    error_message,
                    recorded_at
                )
                VALUES (
                    :pipeline_name,
                    :layer_from,
                    :layer_to,
                    :status,
                    CAST(:data_date AS DATE),
                    :rows_input,
                    :rows_output,
                    :rows_rejected,
                    :dataset_count,
                    :duration_seconds,
                    :error_message,
                    :recorded_at
                )
            """),
            {
                "pipeline_name": args.pipeline_name,
                "layer_from": args.layer_from,
                "layer_to": args.layer_to,
                "status": args.status,
                "data_date": args.data_date,
                "rows_input": parse_optional_int(args.rows_input),
                "rows_output": parse_optional_int(args.rows_output),
                "rows_rejected": parse_optional_int(args.rows_rejected),
                "dataset_count": parse_optional_int(args.dataset_count),
                "duration_seconds": parse_optional_float(args.duration_seconds),
                "error_message": args.error_message,
                "recorded_at": now,
            },
        )

    print("Pipeline metric saved.")


if __name__ == "__main__":
    main()
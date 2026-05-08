import argparse

from sqlalchemy import create_engine, text


POSTGRES_URL = "postgresql+psycopg2://data_user:data_pass@postgres:5432/data_platform"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", nargs="+", required=True)
    args = parser.parse_args()

    engine = create_engine(POSTGRES_URL)

    total = 0

    with engine.connect() as conn:
        for table in args.tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            total += count

    print(total)


if __name__ == "__main__":
    main()
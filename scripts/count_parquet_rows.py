import argparse

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", nargs="+", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("count-parquet-rows")
        .master("local[4]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    total = 0
    for path in args.paths:
        df = spark.read.parquet(path)
        total += df.count()

    print(total)

    spark.stop()


if __name__ == "__main__":
    main()
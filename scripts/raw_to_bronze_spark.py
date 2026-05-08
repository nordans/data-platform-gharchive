from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, lit


RAW_PATH = "/opt/data-platform/data/raw/gharchive/year=2025/month=01/day=01/*.json.gz"
BRONZE_PATH = "/opt/data-platform/data/bronze/gharchive/events/year=2025/month=01/day=01"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("gharchive-raw-to-bronze")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    raw_df = spark.read.json(RAW_PATH)

    bronze_df = (
        raw_df
        .withColumn("source_file", input_file_name())
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_system", lit("gharchive"))
    )

    bronze_df.write.mode("overwrite").parquet(BRONZE_PATH)

    print(f"Bronze saved to: {BRONZE_PATH}")
    print(f"Rows written: {bronze_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
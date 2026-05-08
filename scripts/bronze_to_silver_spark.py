from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    get_json_object,
    to_timestamp,
    to_date,
    hour,
)


BRONZE_PATH = "/opt/data-platform/data/bronze/gharchive/events/year=2025/month=01/day=01"
SILVER_PATH = "/opt/data-platform/data/silver/gharchive/events/year=2025/month=01/day=01"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("gharchive-bronze-to-silver")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    bronze_df = spark.read.parquet(BRONZE_PATH)

    silver_df = (
        bronze_df
        .select(
            get_json_object(col("raw_json"), "$.id").alias("event_id"),
            get_json_object(col("raw_json"), "$.type").alias("event_type"),
            to_timestamp(get_json_object(col("raw_json"), "$.created_at")).alias("created_at"),

            get_json_object(col("raw_json"), "$.actor.id").cast("long").alias("actor_id"),
            get_json_object(col("raw_json"), "$.actor.login").alias("actor_login"),
            get_json_object(col("raw_json"), "$.actor.display_login").alias("actor_display_login"),

            get_json_object(col("raw_json"), "$.repo.id").cast("long").alias("repo_id"),
            get_json_object(col("raw_json"), "$.repo.name").alias("repo_name"),

            get_json_object(col("raw_json"), "$.org.id").cast("long").alias("org_id"),
            get_json_object(col("raw_json"), "$.org.login").alias("org_login"),

            get_json_object(col("raw_json"), "$.public").cast("boolean").alias("is_public"),

            col("source_file"),
            col("ingested_at"),
            col("source_system"),
        )
        .withColumn("event_date", to_date(col("created_at")))
        .withColumn("event_hour", hour(col("created_at")))
        .dropDuplicates(["event_id"])
        .dropna(subset=["event_id", "event_type", "created_at", "repo_name"])
    )

    (
        silver_df
        .repartition(8)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(SILVER_PATH)
    )

    print(f"Silver saved to: {SILVER_PATH}")
    print(f"Rows written: {silver_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
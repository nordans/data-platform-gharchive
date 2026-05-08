from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, hour


BRONZE_PATH = "/opt/data-platform/data/bronze/gharchive/events/year=2025/month=01/day=01"
SILVER_PATH = "/opt/data-platform/data/silver/gharchive/events/year=2025/month=01/day=01"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("gharchive-bronze-to-silver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    bronze_df = spark.read.parquet(BRONZE_PATH)

    silver_df = (
        bronze_df
        .select(
            col("id").alias("event_id"),
            col("type").alias("event_type"),
            to_timestamp(col("created_at")).alias("created_at"),

            col("actor.id").alias("actor_id"),
            col("actor.login").alias("actor_login"),
            col("actor.display_login").alias("actor_display_login"),

            col("repo.id").alias("repo_id"),
            col("repo.name").alias("repo_name"),

            col("org.id").alias("org_id"),
            col("org.login").alias("org_login"),

            col("public").alias("is_public"),
            col("source_file"),
            col("ingested_at"),
            col("source_system"),
        )
        .withColumn("event_date", to_date(col("created_at")))
        .withColumn("event_hour", hour(col("created_at")))
        .dropDuplicates(["event_id"])
        .dropna(subset=["event_id", "event_type", "created_at", "repo_name"])
    )

    silver_df.write.mode("overwrite").parquet(SILVER_PATH)

    print(f"Silver saved to: {SILVER_PATH}")
    print(f"Rows written: {silver_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
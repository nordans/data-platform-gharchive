from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    when,
    lit,
)


SILVER_PATH = "/opt/data-platform/data/silver/gharchive/events/year=2025/month=01/day=01"

GOLD_BI_PATH = "/opt/data-platform/data/gold/gharchive/event_type_hourly/year=2025/month=01/day=01"
GOLD_ANALYTICS_PATH = "/opt/data-platform/data/gold/gharchive/top_repositories_daily/year=2025/month=01/day=01"
GOLD_ML_PATH = "/opt/data-platform/data/gold/gharchive/repo_activity_features/year=2025/month=01/day=01"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("gharchive-silver-to-gold")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    silver_df = spark.read.parquet(SILVER_PATH)

    # GOLD 1: BI — eventy wg typu i godziny
    event_type_hourly = (
        silver_df
        .groupBy("event_date", "event_hour", "event_type")
        .agg(
            count("*").alias("event_count"),
            countDistinct("actor_id").alias("unique_actors"),
            countDistinct("repo_id").alias("unique_repositories"),
        )
        .orderBy("event_date", "event_hour", "event_type")
    )

    (
        event_type_hourly
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(GOLD_BI_PATH)
    )

    # GOLD 2: Analytics — top repozytoria dziennie
    top_repositories_daily = (
        silver_df
        .groupBy("event_date", "repo_id", "repo_name")
        .agg(
            count("*").alias("total_events"),
            countDistinct("actor_id").alias("unique_actors"),
            spark_sum(when(col("event_type") == "PushEvent", 1).otherwise(0)).alias("push_events"),
            spark_sum(when(col("event_type") == "PullRequestEvent", 1).otherwise(0)).alias("pull_request_events"),
            spark_sum(when(col("event_type") == "IssuesEvent", 1).otherwise(0)).alias("issue_events"),
            spark_sum(when(col("event_type") == "WatchEvent", 1).otherwise(0)).alias("watch_events"),
            spark_sum(when(col("event_type") == "ForkEvent", 1).otherwise(0)).alias("fork_events"),
        )
        .orderBy(col("total_events").desc())
    )

    (
        top_repositories_daily
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(GOLD_ANALYTICS_PATH)
    )

    # GOLD 3: AI/ML — feature table per repo
    repo_activity_features = (
        silver_df
        .groupBy("event_date", "repo_id", "repo_name")
        .agg(
            count("*").alias("total_events"),
            countDistinct("actor_id").alias("unique_actors"),
            countDistinct("event_type").alias("event_type_diversity"),
            spark_sum(when(col("event_type") == "PushEvent", 1).otherwise(0)).alias("push_events"),
            spark_sum(when(col("event_type") == "PullRequestEvent", 1).otherwise(0)).alias("pull_request_events"),
            spark_sum(when(col("event_type") == "IssuesEvent", 1).otherwise(0)).alias("issue_events"),
            spark_sum(when(col("event_type") == "WatchEvent", 1).otherwise(0)).alias("watch_events"),
            spark_sum(when(col("event_type") == "ForkEvent", 1).otherwise(0)).alias("fork_events"),
        )
        .withColumn(
            "is_high_activity_repo",
            when(col("total_events") >= 100, lit(1)).otherwise(lit(0))
        )
    )

    (
        repo_activity_features
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(GOLD_ML_PATH)
    )

    print(f"Gold BI saved to: {GOLD_BI_PATH}")
    print(f"Gold analytics saved to: {GOLD_ANALYTICS_PATH}")
    print(f"Gold ML saved to: {GOLD_ML_PATH}")

    print("Rows written:")
    print(f"event_type_hourly: {event_type_hourly.count()}")
    print(f"top_repositories_daily: {top_repositories_daily.count()}")
    print(f"repo_activity_features: {repo_activity_features.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
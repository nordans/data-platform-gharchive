from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, lit


RAW_PATH = "/opt/data-platform/data/raw/gharchive/year=2025/month=01/day=01/*.json.gz"
BRONZE_PATH = "/opt/data-platform/data/bronze/gharchive/events/year=2025/month=01/day=01"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("gharchive-raw-to-bronze")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("parquet.block.size", str(32 * 1024 * 1024))
        .getOrCreate()
    )

    # Czytamy jako tekst, nie jako zagnieżdżony JSON.
    # Każda linia GH Archive = jeden event JSON.
    raw_df = (
        spark.read.text(RAW_PATH)
        .withColumnRenamed("value", "raw_json")
        .withColumn("source_file", input_file_name())
        .withColumn("ingested_at", current_timestamp())
        .withColumn("source_system", lit("gharchive"))
    )

    (
        raw_df
        .repartition(8)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(BRONZE_PATH)
    )

    print(f"Bronze saved to: {BRONZE_PATH}")
    print(f"Rows written: {raw_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()
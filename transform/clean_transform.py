import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, size, concat_ws,
    to_date, filter, transform, lit,
)
from src.utils.logger import get_logger
from src.utils.config import get_config
from src.schemas import MovieSchema

logger = get_logger(__name__)
config = get_config()

# Use config paths with Docker fallback
BRONZE_BASE_PATH = os.getenv("BRONZE_PATH", config.paths.get("bronze", "data/bronze/movies"))
SILVER_PATH = os.getenv("SILVER_PATH", config.paths.get("silver", "data/silver/movies_curated"))

# Handle Docker environment
if os.path.exists("/opt/app/data"):
    BRONZE_BASE_PATH = "/opt/app/data/bronze/movies"
    SILVER_PATH = "/opt/app/data/silver/movies_curated"

DONE_DIR = os.path.join(SILVER_PATH, ".done")


def find_unprocessed_bronze_files(bronze_base: str) -> list:
    """
    Return list of (date, bronze_path, stem) for every bronze parquet dir
    that has no corresponding .done marker in silver.
    CLI arg / INGESTION_DATE env var pins processing to a single date.
    """
    pin_date = sys.argv[1] if len(sys.argv) > 1 else os.getenv("INGESTION_DATE")

    date_partitions = sorted([
        d for d in os.listdir(bronze_base)
        if d.startswith("ingestion_date=") and os.path.isdir(os.path.join(bronze_base, d))
    ])

    if not date_partitions:
        raise FileNotFoundError(f"No ingestion_date partitions found in {bronze_base}")

    unprocessed = []
    for partition in date_partitions:
        date = partition.split("=", 1)[1]
        if pin_date and date != pin_date:
            continue
        partition_dir = os.path.join(bronze_base, partition)
        parquet_dirs = sorted([
            d for d in os.listdir(partition_dir)
            if d.endswith(".parquet") and os.path.isdir(os.path.join(partition_dir, d))
        ])
        for pq_dir in parquet_dirs:
            stem = pq_dir[: -len(".parquet")]
            marker = os.path.join(DONE_DIR, f"{date}__{stem}")
            if not os.path.exists(marker):
                unprocessed.append((date, os.path.join(partition_dir, pq_dir), stem))

    return unprocessed


def mark_done(date: str, stem: str):
    os.makedirs(DONE_DIR, exist_ok=True)
    open(os.path.join(DONE_DIR, f"{date}__{stem}"), "w").close()


def transform_bronze(df, spark):
    initial_count = df.count()
    logger.info(f"Initial row count: {initial_count}")

    # Filter Released movies
    if "status" in df.columns:
        df = df.filter(col("status") == "Released")

    # Drop irrelevant columns
    drop_cols = ["adult", "imdb_id", "original_title", "video", "homepage", "status"]
    df = df.drop(*[c for c in drop_cols if c in df.columns])

    # Flatten JSON-like columns
    df = (
        df
        .withColumn("belongs_to_collection", col("belongs_to_collection.name"))
        .withColumn("genres",
            concat_ws("|", transform(col("genres"), lambda x: x["name"])))
        .withColumn("production_companies",
            concat_ws("|", transform(col("production_companies"), lambda x: x["name"])))
        .withColumn("production_countries",
            concat_ws("|", transform(col("production_countries"), lambda x: x["iso_3166_1"])))
        .withColumn("spoken_languages",
            concat_ws("|", transform(col("spoken_languages"), lambda x: x["iso_639_1"])))
    )

    # Extract cast & director
    df = (
        df
        .withColumn("cast",
            concat_ws("|", transform(col("credits.cast"), lambda x: x["name"])))
        .withColumn("cast_size", size(col("credits.cast")))
        .withColumn(
            "_director_array",
            transform(
                filter(col("credits.crew"), lambda x: x["job"] == "Director"),
                lambda x: x["name"]
            )
        )
        .withColumn(
            "director",
            when(size(col("_director_array")) > 0, col("_director_array")[0]).otherwise(None)
        )
        .drop("_director_array")
        .withColumn("crew_size", size(col("credits.crew")))
        .drop("credits")
    )

    # Type casting
    numeric_cols = MovieSchema.get_numeric_columns()
    for col_name, dtype in numeric_cols.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(dtype))

    df = df.withColumn("release_date", to_date(col("release_date")))

    # Handle unrealistic values
    df = (
        df
        .withColumn("budget", when(col("budget") <= 0, None).otherwise(col("budget")))
        .withColumn("revenue", when(col("revenue") <= 0, None).otherwise(col("revenue")))
        .withColumn("runtime", when(col("runtime") <= 0, None).otherwise(col("runtime")))
    )

    # Convert to million USD
    df = (
        df
        .withColumn("budget_musd", col("budget") / 1_000_000)
        .withColumn("revenue_musd", col("revenue") / 1_000_000)
        .drop("budget", "revenue")
    )

    # Vote handling
    df = df.withColumn(
        "vote_average",
        when(col("vote_count") == 0, None).otherwise(col("vote_average"))
    )

    # Replace placeholder text
    df = (
        df
        .withColumn("overview", when(col("overview") == "No Data", None).otherwise(col("overview")))
        .withColumn("tagline", when(col("tagline") == "No Data", None).otherwise(col("tagline")))
    )

    # Remove duplicates & invalid rows
    df = df.dropDuplicates(["id"])
    df = df.dropna(subset=["id", "title"])

    # Keep rows with at least 10 non-null columns
    non_null_expr = sum(col(c).isNotNull().cast("int") for c in df.columns)
    df = df.filter(non_null_expr >= 10)

    # Reorder columns
    final_columns = [
        "id", "title", "tagline", "release_date", "genres",
        "belongs_to_collection", "original_language",
        "budget_musd", "revenue_musd",
        "production_companies", "production_countries",
        "vote_count", "vote_average", "popularity",
        "runtime", "overview", "spoken_languages",
        "poster_path", "cast", "cast_size",
        "director", "crew_size",
    ]
    df = df.select(*[c for c in final_columns if c in df.columns])

    return df, initial_count


def main():
    spark = (
        SparkSession.builder
        .appName("movie_data_clean_transforma")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Starting data cleaning & transformation job")

    files = find_unprocessed_bronze_files(BRONZE_BASE_PATH)
    if not files:
        logger.info("No unprocessed bronze files found. Nothing to do.")
        spark.stop()
        return

    logger.info(f"Found {len(files)} unprocessed bronze file(s).")

    for date, bronze_path, stem in files:
        logger.info(f"Processing ingestion_date={date} | file={stem}")

        df = spark.read.parquet(bronze_path)
        df.cache()

        df, initial_count = transform_bronze(df, spark)

        # Tag with ingestion_date so silver is properly Hive-partitioned
        df = df.withColumn("ingestion_date", lit(date))

        final_count = df.count()
        (df.write
           .mode("append")
           .partitionBy("ingestion_date")
           .parquet(SILVER_PATH))

        df.unpersist()
        mark_done(date, stem)

        logger.info(f"Written to silver | ingestion_date={date} | rows={final_count} | dropped={initial_count - final_count}")

    spark.stop()
    logger.info("Transformation job completed successfully")


if __name__ == "__main__":
    main()

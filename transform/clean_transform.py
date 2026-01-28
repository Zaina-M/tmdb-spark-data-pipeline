import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, size, explode, concat_ws,
    to_date, year, array, lit, filter, transform
)
from pyspark.sql.types import DoubleType, IntegerType
from src.utils.logger import get_logger

logger = get_logger(__name__)



BRONZE_BASE_PATH = "/opt/app/data/bronze/movies"
SILVER_PATH = "/opt/app/data/silver/movies_curated"


def main():
    spark = (
        SparkSession.builder
        .appName("movie_data_clean_transforma")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Starting data cleaning & transformation job")


    # 1. Read Bronze (all ingestion dates)
    df = spark.read.option("multiline", "true").json(BRONZE_BASE_PATH)
    
    # Cache to avoid recomputing when counting
    df.cache()
    initial_count = df.count()
    logger.info(f"Initial row count: {initial_count}")

     
    # 2. Filter Released movies
    if "status" in df.columns:
        df = df.filter(col("status") == "Released")

    
    # 3. Drop irrelevant columns
    
    drop_cols = [
        "adult", "imdb_id", "original_title",
        "video", "homepage", "status"
    ]

    df = df.drop(*[c for c in drop_cols if c in df.columns])

    
    # 4. Flatten JSON-like columns
    
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
    
    # 5. Extract cast & director
    df = (
        df
        .withColumn("cast",
            concat_ws("|", transform(col("credits.cast"), lambda x: x["name"])))
        .withColumn("cast_size", size(col("credits.cast")))
        .withColumn(
            "director",
            transform(
                filter(col("credits.crew"), lambda x: x["job"] == "Director"),
                lambda x: x["name"]
            )[0]
        )
        .withColumn("crew_size", size(col("credits.crew")))
        .drop("credits")
    )

    
    # 6. Type casting
    
    numeric_cols = {
        "budget": DoubleType(),
        "revenue": DoubleType(),
        "popularity": DoubleType(),
        "vote_count": IntegerType(),
        "vote_average": DoubleType(),
        "runtime": IntegerType(),
        "id": IntegerType()
    }

    for col_name, dtype in numeric_cols.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, col(col_name).cast(dtype))

    df = df.withColumn("release_date", to_date(col("release_date")))


    # 7. Handle unrealistic values
    
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

    
    # 7. Remove duplicates & invalid rows
    
    df = df.dropDuplicates(["id"])
    df = df.dropna(subset=["id", "title"])

    
    # 8. Keep rows with at least 10 non-null columns
  
    non_null_expr = sum(col(c).isNotNull().cast("int") for c in df.columns)
    df = df.filter(non_null_expr >= 10)

    # 9. Calculate ROI
    df = df.withColumn(
        "roi",
        when(col("budget_musd") > 0  , col("revenue_musd") / col("budget_musd") )
             
)
    
    # 10. Reorder columns

    final_columns = [
        "id", "title", "tagline", "release_date", "genres",
        "belongs_to_collection", "original_language",
        "budget_musd", "revenue_musd","roi",
        "production_companies", "production_countries",
        "vote_count", "vote_average", "popularity",
        "runtime", "overview", "spoken_languages",
        "poster_path", "cast", "cast_size",
        "director", "crew_size"
    ]

    df = df.select(*[c for c in final_columns if c in df.columns])

    
    # 11. Write curated dataset(silver) + log metrics
    final_count = df.count()  # Single count before writing
    df.write.mode("overwrite").parquet(SILVER_PATH)
    df.unpersist()  # Release cached data AFTER writing

    logger.info(f"Curated dataset written to {SILVER_PATH}")
    logger.info(f"Final row count: {final_count}")
    logger.info(f"Rows dropped: {initial_count - final_count}")

    spark.stop()
    logger.info("Transformation job completed successfully")


if __name__ == "__main__":
    main()

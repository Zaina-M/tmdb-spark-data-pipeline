import os
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, desc, asc, expr, round,
)
from pyspark.sql.types import StringType
from src.utils.logger import get_logger
from src.utils.config import get_config

logger = get_logger(__name__)
config = get_config()

# PATHS - Use config with Docker fallback
SILVER_PATH = os.getenv("SILVER_PATH", config.paths.get("silver", "data/silver/movies_curated"))
GOLD_PATH = os.getenv("GOLD_PATH", config.paths.get("gold", "data/gold"))

# Handle Docker environment
if os.path.exists("/opt/app/data"):
    SILVER_PATH = "/opt/app/data/silver/movies_curated"
    GOLD_PATH = "/opt/app/data/gold"

DONE_DIR = os.path.join(GOLD_PATH, ".done")


def find_unprocessed_silver_dates(silver_base: str) -> list:
    """Return sorted list of ingestion_date values present in silver but not yet KPI'd."""
    pin_date = sys.argv[1] if len(sys.argv) > 1 else os.getenv("INGESTION_DATE")

    if not os.path.isdir(silver_base):
        raise FileNotFoundError(f"Silver path not found: {silver_base}")

    dates = sorted([
        d.split("=", 1)[1]
        for d in os.listdir(silver_base)
        if d.startswith("ingestion_date=") and os.path.isdir(os.path.join(silver_base, d))
    ])

    if not dates:
        raise FileNotFoundError(f"No ingestion_date partitions found in {silver_base}")

    return [
        date for date in dates
        if (not pin_date or date == pin_date)
        and not os.path.exists(os.path.join(DONE_DIR, date))
    ]


def mark_done(date: str):
    os.makedirs(DONE_DIR, exist_ok=True)
    open(os.path.join(DONE_DIR, date), "w").close()


def prepare_kpis(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("profit_musd", col("revenue_musd") - col("budget_musd"))
        .withColumn(
            "roi",
            when(col("budget_musd") >= 10, col("revenue_musd") / col("budget_musd"))
        )
    )


def rank_movies(
    df: DataFrame,
    metric_col: str,
    order: str = "desc",
    top_n: int = 5,
    filter_expr=None
) -> DataFrame:
    df = prepare_kpis(df)
    if filter_expr is not None:
        df = df.filter(filter_expr)
    ordering = desc(metric_col) if order == "desc" else asc(metric_col)
    return df.orderBy(ordering).limit(top_n)


def run_movie_kpis(df: DataFrame, gold_path: str):
    kpis = {
        "highest_revenue": rank_movies(df, "revenue_musd", "desc", 5),
        "highest_budget":  rank_movies(df, "budget_musd",  "desc", 5),
        "highest_profit":  rank_movies(df, "profit_musd",  "desc", 5),
        "lowest_profit":   rank_movies(df, "profit_musd",  "asc",  5),
        "highest_roi":     rank_movies(df, "roi", "desc", 5, col("budget_musd") >= 10),
        "lowest_roi":      rank_movies(df, "roi", "asc",  5, col("budget_musd") >= 10),
        "most_voted":      rank_movies(df, "vote_count",   "desc", 5),
        "highest_rated":   rank_movies(df, "vote_average", "desc", 5, col("vote_count") >= 10),
        "lowest_rated":    rank_movies(df, "vote_average", "asc",  5, col("vote_count") >= 10),
        "most_popular":    rank_movies(df, "popularity",   "desc", 5),
    }

    for name, result_df in kpis.items():
        output_path = os.path.join(gold_path, name)
        result_df.write.mode("overwrite").parquet(output_path)
        logger.info(f"KPI written: {output_path}")


def run_search_queries(df: DataFrame, gold_path: str):
    df = prepare_kpis(df)

    search_1 = (
        df.filter(
            (col("genres").contains("Science Fiction")) &
            (col("genres").contains("Action")) &
            (col("cast").contains("Bruce Willis")) &
            (col("vote_count") >= 10)
        )
        .orderBy(desc("vote_average"))
    )
    search_1.write.mode("overwrite").parquet(os.path.join(gold_path, "search_bruce_willis"))

    search_2 = (
        df.filter(
            (col("cast").contains("Uma Thurman")) &
            (col("director") == "Quentin Tarantino")
        )
        .orderBy(asc("runtime"))
    )
    search_2.write.mode("overwrite").parquet(os.path.join(gold_path, "search_tarantino_uma"))

    logger.info("Advanced search queries completed")


def franchise_vs_standalone(df: DataFrame, gold_path: str):
    df = prepare_kpis(df)

    df = df.withColumn(
        "is_franchise",
        when(col("belongs_to_collection").isNotNull(), "Franchise").otherwise("Standalone")
    )

    stats = df.groupBy("is_franchise").agg(
        round(expr("avg(revenue_musd)"), 2).alias("mean_revenue"),
        round(expr("percentile_approx(roi, 0.5)"), 2).alias("median_roi"),
        round(expr("avg(budget_musd)"), 2).alias("mean_budget"),
        round(expr("avg(popularity)"), 2).alias("mean_popularity"),
        round(expr("avg(vote_average)"), 2).alias("mean_rating")
    )

    stats.write.mode("overwrite").parquet(os.path.join(gold_path, "franchise_vs_standalone"))
    logger.info("Franchise vs Standalone analysis written")


def top_franchises(df: DataFrame, gold_path: str):
    df = prepare_kpis(df)

    stats = (
        df.filter(col("belongs_to_collection").isNotNull())
        .groupBy("belongs_to_collection")
        .agg(
            expr("count(*)").alias("num_movies"),
            round(expr("sum(budget_musd)"), 2).alias("total_budget"),
            round(expr("avg(budget_musd)"), 2).alias("mean_budget"),
            round(expr("sum(revenue_musd)"), 2).alias("total_revenue"),
            round(expr("avg(revenue_musd)"), 2).alias("mean_revenue"),
            round(expr("avg(vote_average)"), 2).alias("mean_rating")
        )
        .orderBy(desc("total_revenue"))
    )

    stats.write.mode("overwrite").parquet(os.path.join(gold_path, "top_franchises"))
    logger.info("Top franchises written")


def top_directors(df: DataFrame, gold_path: str):
    df = prepare_kpis(df)

    stats = (
        df.groupBy("director")
        .agg(
            expr("count(*)").alias("num_movies"),
            round(expr("sum(revenue_musd)"), 2).alias("total_revenue"),
            round(expr("avg(vote_average)"), 2).alias("mean_rating")
        )
        .orderBy(desc("total_revenue"))
    )

    stats.write.mode("overwrite").parquet(os.path.join(gold_path, "top_directors"))
    logger.info("Top directors written")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieKPIJob").getOrCreate()
    logger.info("Starting KPI Job")

    dates = find_unprocessed_silver_dates(SILVER_PATH)
    if not dates:
        logger.info("No unprocessed silver partitions found. Nothing to do.")
        spark.stop()
        exit(0)

    logger.info(f"Found {len(dates)} unprocessed silver partition(s).")

    for date in dates:
        logger.info(f"Running KPIs for ingestion_date={date}")
        try:
            silver_partition = os.path.join(SILVER_PATH, f"ingestion_date={date}")
            gold_partition = os.path.join(GOLD_PATH, f"ingestion_date={date}")

            movies_df = spark.read.parquet(silver_partition)
            logger.info(f"Loaded {movies_df.count()} rows for ingestion_date={date}")

            run_movie_kpis(movies_df, gold_partition)
            run_search_queries(movies_df, gold_partition)
            franchise_vs_standalone(movies_df, gold_partition)
            top_franchises(movies_df, gold_partition)
            top_directors(movies_df, gold_partition)

            mark_done(date)
            logger.info(f"KPIs complete for ingestion_date={date}")
        except Exception as e:
            logger.error(f"KPI job failed for ingestion_date={date}: {e}")

    spark.stop()
    logger.info("KPI job completed successfully")

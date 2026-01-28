from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, desc, asc, expr, round, 
)
from pyspark.sql.types import StringType
from src.utils.logger import get_logger

logger = get_logger(__name__)


# PATHS

SILVER_PATH = "/opt/app/data/silver/movies_curated"
GOLD_PATH = "/opt/app/data/gold"

def prepare_kpis(df: DataFrame) -> DataFrame:

    # Adds profit and ROI columns using Spark-native expressions.
    return (
        df
        .withColumn("profit_musd", col("revenue_musd") - col("budget_musd"))
        .withColumn(
            "roi",
            when(col("budget_musd") >= 10, col("revenue_musd") / col("budget_musd"))
        )
    )


# 3. GENERIC RANKING FUNCTION

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


# 4. KPI DEFINITIONS (BEST / WORST MOVIES)

def run_movie_kpis(df: DataFrame, gold_path: str):

    kpis = {
        "highest_revenue": rank_movies(
            df, "revenue_musd", "desc", 5
        ),
        "highest_budget": rank_movies(
            df, "budget_musd", "desc", 5
        ),
        "highest_profit": rank_movies(
            df, "profit_musd", "desc", 5
        ),
        "lowest_profit": rank_movies(
            df, "profit_musd", "asc", 5
        ),
        "highest_roi": rank_movies(
            df, "roi", "desc", 5, col("budget_musd") >= 10
        ),
        "lowest_roi": rank_movies(
            df, "roi", "asc", 5, col("budget_musd") >= 10
        ),
        "most_voted": rank_movies(
            df, "vote_count", "desc", 5
        ),
        "highest_rated": rank_movies(
            df, "vote_average", "desc", 5, col("vote_count") >= 10
        ),
        "lowest_rated": rank_movies(
            df, "vote_average", "asc", 5, col("vote_count") >= 10
        ),
        "most_popular": rank_movies(
            df, "popularity", "desc", 5
        )
    }

    for name, result_df in kpis.items():
        output_path = f"{gold_path}/{name}"
        result_df.write.mode("overwrite").parquet(output_path)
        logger.info(f"KPI written: {output_path}")



# 5. ADVANCED SEARCH QUERIES

def run_search_queries(df: DataFrame, gold_path: str):
    df = prepare_kpis(df)

    # Search 1:
    # Best-rated Science Fiction Action movies starring Bruce Willis
    search_1 = (
        df.filter(
            (col("genres").contains("Science Fiction")) &
            (col("genres").contains("Action")) &
            (col("cast").contains("Bruce Willis")) &
            (col("vote_count") >= 10)
        )
        .orderBy(desc("vote_average"))
    )

    search_1.write.mode("overwrite").parquet(f"{gold_path}/search_bruce_willis")

    # Search 2:
    # Movies starring Uma Thurman, directed by Quentin Tarantino
    search_2 = (
        df.filter(
            (col("cast").contains("Uma Thurman")) &
            (col("director") == "Quentin Tarantino")
        )
        .orderBy(asc("runtime"))
    )

    search_2.write.mode("overwrite").parquet(f"{gold_path}/search_tarantino_uma")

    logger.info("Advanced search queries completed")



# 6. FRANCHISE VS STANDALONE ANALYSIS

def franchise_vs_standalone(df: DataFrame, gold_path: str):
    df = prepare_kpis(df)

    df = df.withColumn(
        "is_franchise",
        when(col("belongs_to_collection").isNotNull(), "Franchise")
        .otherwise("Standalone")
    )

    stats = df.groupBy("is_franchise").agg(
        round(expr("avg(revenue_musd)"), 2).alias("mean_revenue"),
        round(expr("percentile_approx(roi, 0.5)"), 2).alias("median_roi"),
        round(expr("avg(budget_musd)"), 2).alias("mean_budget"),
        round(expr("avg(popularity)"), 2).alias("mean_popularity"),
        round(expr("avg(vote_average)"), 2).alias("mean_rating")
    )

    stats.write.mode("overwrite").parquet(f"{gold_path}/franchise_vs_standalone")
    logger.info("Franchise vs Standalone analysis written")



# 7. MOST SUCCESSFUL FRANCHISES

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

    stats.write.mode("overwrite").parquet(f"{gold_path}/top_franchises")
    logger.info("Top franchises written")



# 8. MOST SUCCESSFUL DIRECTORS

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

    stats.write.mode("overwrite").parquet(f"{gold_path}/top_directors")
    logger.info("Top directors written")



# 9. ENTRY POINT

if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieKPIJob").getOrCreate()

    logger.info("Starting KPI Job")

    movies_df = spark.read.parquet(SILVER_PATH)
    logger.info(f"Loaded SILVER dataset: {movies_df.count()} rows")

    run_movie_kpis(movies_df, GOLD_PATH)
    run_search_queries(movies_df, GOLD_PATH)
    franchise_vs_standalone(movies_df, GOLD_PATH)
    top_franchises(movies_df, GOLD_PATH)
    top_directors(movies_df, GOLD_PATH)

    spark.stop()
    logger.info("KPI job completed successfully")

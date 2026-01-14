# from pyspark.sql import DataFrame, SparkSession
# from pyspark.sql.functions import (
#     col, when, desc, asc, expr, round, udf
# )
# from pyspark.sql.types import DoubleType
# from src.utils.logger import get_logger

# logger = get_logger(__name__)

# #Paths
# SILVER_PATH = "/opt/app/data/silver/movies_curated"
# GOLD_PATH = "/opt/app/data/gold"


# def prepare_kpis(df) -> DataFrame:
#     """
#     Adds computed KPI columns: profit and ROI.
#     """
#     df = df.withColumn("profit_musd", col("revenue_musd") - col("budget_musd")) \
#            .withColumn("roi", when(col("budget_musd") >= 10, col("revenue_musd") / col("budget_musd")).otherwise(None))
#     return df

# def get_top_movies(df: DataFrame, metric: str, top_n: int = 5, min_votes: int = 10, budget_filter: float = None) -> DataFrame:
#     """
#     Returns the top_n movies based on a chosen metric.
#     metric options: 'highest_rated', 'most_popular', 'highest_revenue', 'highest_profit', 'highest_roi', 'most_voted'
#     """
#     df = prepare_kpis(df)
    
#     if metric == "highest_rated":
#         df_filtered = df.filter(col("vote_count") >= min_votes)
#         result = df_filtered.orderBy(desc("vote_average")).limit(top_n)
#     elif metric == "most_popular":
#         result = df.orderBy(desc("popularity")).limit(top_n)
#     elif metric == "highest_revenue":
#         result = df.orderBy(desc("revenue_musd")).limit(top_n)
#     elif metric == "highest_profit":
#         result = df.orderBy(desc("profit_musd")).limit(top_n)
#     elif metric == "highest_roi":
#         if budget_filter:
#             df_filtered = df.filter(col("budget_musd") >= budget_filter)
#         else:
#             df_filtered = df.filter(col("budget_musd") >= 10)
#         result = df_filtered.orderBy(desc("roi")).limit(top_n)
#     elif metric == "most_voted":
#         result = df.orderBy(desc("vote_count")).limit(top_n)
#     else:
#         raise ValueError(f"Metric '{metric}' not recognized. Choose a valid KPI.")
    
    
#     return result

# def get_franchise_stats(df, output_file: str = None):
#     """Compute franchise vs standalone performance"""
#     df = prepare_kpis(df)
#     stats = df.groupBy("is_franchise").agg(
#         round(expr("avg(revenue_musd)"),2).alias("mean_revenue"),
#         round(expr("percentile_approx(roi,0.5)"),2).alias("median_roi"),
#         round(expr("avg(budget_musd)"),2).alias("mean_budget"),
#         round(expr("avg(popularity)"),2).alias("mean_popularity"),
#         round(expr("avg(vote_average)"),2).alias("mean_rating")
#     )
#     if output_file:
#         stats.write.mode("overwrite").parquet(output_file)
#         logger.info(f"Franchise stats written to {output_file}")
#     return stats

# def get_top_franchises(df, top_n: int = 5, output_file: str = None):
#     """Most successful franchises"""
#     df = prepare_kpis(df)
#     stats = df.filter(col("belongs_to_collection").isNotNull()) \
#               .groupBy("belongs_to_collection").agg(
#                   expr("count(*)").alias("num_movies"),
#                   round(expr("sum(budget_musd)"),2).alias("total_budget"),
#                   round(expr("sum(revenue_musd)"),2).alias("total_revenue"),
#                   round(expr("avg(revenue_musd)"),2).alias("mean_revenue"),
#                   round(expr("avg(vote_average)"),2).alias("mean_rating")
#               ).orderBy(desc("total_revenue"))
#     if output_file:
#         stats.write.mode("overwrite").parquet(output_file)
#         logger.info(f"Top franchises written to {output_file}")
#     return stats

# def get_top_directors(df, top_n: int = 5, output_file: str = None):
#     """Most successful directors"""
#     df = prepare_kpis(df)
#     stats = df.groupBy("director").agg(
#         expr("count(*)").alias("num_movies"),
#         round(expr("sum(revenue_musd)"),2).alias("total_revenue"),
#         round(expr("avg(vote_average)"),2).alias("mean_rating")
#     ).orderBy(desc("total_revenue"))
#     if output_file:
#         stats.write.mode("overwrite").parquet(output_file)
#         logger.info(f"Top directors written to {output_file}")
#     return stats




#     # KPI 
# KPI_REGISTRY = {
#     "top_rated_movies": {
#         "metric": "highest_rated",
#         "params": {"top_n": 5}
#     },
#     "top_popular_movies": {
#         "metric": "most_popular",
#         "params": {"top_n": 5}
#     },
#     "top_revenue_movies": {
#         "metric": "highest_revenue",
#         "params": {"top_n": 5}
#     },
#     "top_profit_movies": {
#         "metric": "highest_profit",
#         "params": {"top_n": 5}
#     },
#     "top_roi_movies": {
#         "metric": "highest_roi",
#         "params": {"top_n": 5, "budget_filter": 10}
#     },
#     "top_voted_movies": {
#         "metric": "most_voted",
#         "params": {"top_n": 5}
#     }
# }


# # Run and save all KPIs
# def run_kpis(df: DataFrame, gold_path: str):
#     for kpi_name, cfg in KPI_REGISTRY.items():
#         logger.info(f"Running KPI: {kpi_name}")

#         result_df = get_top_movies(
#             df,
#             metric=cfg["metric"],
#             **cfg["params"]
#         )

#         output_path = f"{gold_path}/{kpi_name}"
#         result_df.write.mode("overwrite").parquet(output_path)

#         logger.info(f"KPI {kpi_name} written to {output_path}")

# # Entry point
# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("MovieKPIJob").getOrCreate()

#     logger.info("Starting KPI job")

#     movies_df = spark.read.parquet(SILVER_PATH)
#     logger.info(f"Loaded SILVER dataset: {movies_df.count()} rows")

#     run_kpis(movies_df, GOLD_PATH)

#     spark.stop()
#     logger.info("KPI job completed successfully")



from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, desc, asc, expr, round, 
)
from pyspark.sql.types import StringType
from src.utils.logger import get_logger

logger = get_logger(__name__)

# --------------------------------------------------
# PATHS
# --------------------------------------------------
SILVER_PATH = "/opt/app/data/silver/movies_curated"
GOLD_PATH = "/opt/app/data/gold"

# --------------------------------------------------
# 1. USER DEFINED FUNCTION (MINIMAL & SAFE)
# --------------------------------------------------
# @udf(StringType())
# def rating_bucket(rating):
#     """
#     Simple UDF to classify ratings.
#     Included to satisfy assignment requirement.
#     """
#     if rating is None:
#         return "Unknown"
#     if rating >= 7:
#         return "High"
#     if rating >= 5:
#         return "Medium"
#     return "Low"


# --------------------------------------------------
# 2. KPI PREPARATION (NO UDF USED HERE)
# --------------------------------------------------
def prepare_kpis(df: DataFrame) -> DataFrame:
    """
    Adds profit and ROI columns using Spark-native expressions.
    """
    return (
        df
        .withColumn("profit_musd", col("revenue_musd") - col("budget_musd"))
        .withColumn(
            "roi",
            when(col("budget_musd") >= 10, col("revenue_musd") / col("budget_musd"))
        )
    )

# --------------------------------------------------
# 3. GENERIC RANKING FUNCTION
# --------------------------------------------------
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


# --------------------------------------------------
# 4. KPI DEFINITIONS (BEST / WORST MOVIES)
# --------------------------------------------------
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


# --------------------------------------------------
# 5. ADVANCED SEARCH QUERIES
# --------------------------------------------------
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


# --------------------------------------------------
# 6. FRANCHISE VS STANDALONE ANALYSIS
# --------------------------------------------------
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


# --------------------------------------------------
# 7. MOST SUCCESSFUL FRANCHISES
# --------------------------------------------------
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


# --------------------------------------------------
# 8. MOST SUCCESSFUL DIRECTORS
# --------------------------------------------------
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


# --------------------------------------------------
# 9. ENTRY POINT
# --------------------------------------------------
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

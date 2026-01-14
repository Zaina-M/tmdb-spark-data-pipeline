import os
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col
from src.utils.logger import get_logger

logger = get_logger(__name__)

SILVER_PATH = "/opt/app/data/silver/movies_curated"
GOLD_PATH = "/opt/app/data/gold"
VISUAL_PATH = "/opt/app/data/visualizations"

os.makedirs(VISUAL_PATH, exist_ok=True)


def spark_to_pandas(df, cols):
    """Safely convert selected Spark columns to Pandas"""
    return df.select(*cols).toPandas()


# --------------------------------------------------
# 1. Revenue vs Budget
# --------------------------------------------------
def plot_revenue_vs_budget(df):
    pdf = spark_to_pandas(df, ["budget_musd", "revenue_musd"])

    plt.figure()
    plt.scatter(pdf["budget_musd"], pdf["revenue_musd"])
    plt.xlabel("Budget (Million USD)")
    plt.ylabel("Revenue (Million USD)")
    plt.title("Revenue vs Budget")
    plt.tight_layout()
    plt.savefig(f"{VISUAL_PATH}/revenue_vs_budget.png")
    plt.close()


# --------------------------------------------------
# 2. ROI Distribution by Genre
# --------------------------------------------------
def plot_roi_by_genre(df):
    # Convert only required columns
    pdf = spark_to_pandas(df, ["genres", "roi"]).dropna()

    # Split multiple genres
    pdf["genre"] = pdf["genres"].str.split("|")
    pdf = pdf.explode("genre")

    # Aggregate mean ROI per genre
    genre_roi = (
        pdf.groupby("genre")["roi"]
        .mean()
        .sort_values(ascending=False)
    )

    # Plot
    plt.figure()
    genre_roi.plot(kind="bar", label="Mean ROI")
    plt.xlabel("Genre")
    plt.ylabel("Mean ROI")
    plt.title("Average Return on Investment (ROI) by Genre")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{VISUAL_PATH}/roi_by_genre_bar.png")
    plt.close()



# --------------------------------------------------
# 3. Popularity vs Rating
# --------------------------------------------------
def plot_popularity_vs_rating(df):
    pdf = spark_to_pandas(df, ["popularity", "vote_average"])

    plt.figure()
    plt.scatter(pdf["popularity"], pdf["vote_average"])
    plt.xlabel("Popularity")
    plt.ylabel("Rating")
    plt.title("Popularity vs Rating")
    plt.tight_layout()
    plt.savefig(f"{VISUAL_PATH}/popularity_vs_rating.png")
    plt.close()


# --------------------------------------------------
# 4. Yearly Box Office Trends
# --------------------------------------------------
def plot_yearly_trends(df):
    yearly_df = (
        df.withColumn("year", year(col("release_date")))
          .groupBy("year")
          .avg("revenue_musd")
          .orderBy("year")
    )

    pdf = yearly_df.toPandas()

    plt.figure()
    plt.plot(pdf["year"], 
             pdf["avg(revenue_musd)"],
             label = "Average Revenue (Million USD)")
    plt.xlabel("Year")
    plt.ylabel("Average Revenue ")
    plt.title("Yearly Box Office Trends")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{VISUAL_PATH}/yearly_trends.png")
    plt.close()


# --------------------------------------------------
# 5. Franchise vs Standalone Comparison
# --------------------------------------------------
# def plot_franchise_vs_standalone():
#     pdf = pd.read_parquet(f"{GOLD_PATH}/franchise_vs_standalone")

#     metrics = {
#         "mean_revenue": "Mean Revenue (Million USD)",
#         "mean_popularity": "Mean Popularity",
#         "mean_rating": "Mean Rating"
#     }

#     for metric, ylabel in metrics.items():
#         plt.figure()
#         plt.bar(
#             pdf["is_franchise"].astype(str),
#             pdf[metric],
#             label=metric.replace("_", " ").title()
#         )
#         plt.xlabel("Movie Type")
#         plt.ylabel(ylabel)
#         plt.title(f"Franchise vs Standalone: {ylabel}")
#         plt.legend()
#         plt.tight_layout()
#         plt.savefig(f"{VISUAL_PATH}/franchise_vs_standalone_{metric}.png")
#         plt.close()

def plot_franchise_vs_standalone():
    pdf = pd.read_parquet(f"{GOLD_PATH}/franchise_vs_standalone")

    # Labels: 'is_franchise' is already "Franchise" or "Standalone"
    pdf["label"] = pdf["is_franchise"]

    # Colors: Map string values to colors
    colors = pdf["is_franchise"].map({
        "Franchise": "skyblue",
        "Standalone": "orange"
    }).tolist()

    metrics = {
        "mean_revenue": "Mean Revenue (Million USD)",
        "mean_popularity": "Mean Popularity",
        "mean_rating": "Mean Rating"
    }

    for metric, ylabel in metrics.items():
        plt.figure()
        plt.bar(
            pdf["label"],
            pdf[metric],
            color=colors
        )
        plt.xlabel("Movie Type")
        plt.ylabel(ylabel)
        plt.title(f"Franchise vs Standalone: {ylabel}")
        plt.tight_layout()
        plt.savefig(f"{VISUAL_PATH}/franchise_vs_standalone_{metric}.png")
        plt.close()



# --------------------------------------------------
# ENTRY POINT
# --------------------------------------------------
if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieVisualization").getOrCreate()

    logger.info("Starting visualization job")

    movies_df = spark.read.parquet(SILVER_PATH)

    plot_revenue_vs_budget(movies_df)
    plot_roi_by_genre(movies_df)
    plot_popularity_vs_rating(movies_df)
    plot_yearly_trends(movies_df)
    plot_franchise_vs_standalone()

    spark.stop()
    logger.info("All visualizations generated successfully")

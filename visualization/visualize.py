import os
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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
    return df.select(*cols).toPandas()


# 1. Revenue vs Budget
def plot_revenue_vs_budget(df):
    pdf = spark_to_pandas(df, ["budget_musd", "revenue_musd"]).dropna()

    plt.figure(figsize=(12, 6))

    plt.scatter(
        pdf["budget_musd"],
        pdf["revenue_musd"],
        alpha=0.6,
        edgecolors="k",
        linewidth=0.5,
        s=100,
        label="Movies"
    )


    plt.xlabel("Budget (Million USD)")
    plt.ylabel("Revenue (Million USD)")
    plt.title("Revenue vs Budget", fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.legend()

    plt.tight_layout()
    plt.savefig(f"{VISUAL_PATH}/revenue_vs_budget.png", dpi=300)
    plt.close()


# 2. ROI Distribution by Genre
def plot_roi_by_genre(df):
    pdf = spark_to_pandas(df, ["genres", "roi"]).dropna()

    pdf["genre"] = pdf["genres"].str.split("|")
    pdf = pdf.explode("genre")

    genre_roi = (
        pdf.groupby("genre")["roi"]
        .mean()
        .sort_values(ascending=False)
    )

    plt.figure(figsize=(12, 6))

    bars = plt.bar(
        range(len(genre_roi)),
        genre_roi.values,
        edgecolor="black",
        linewidth=1.2,
        label="Genres"
    )

    plt.xticks(range(len(genre_roi)), genre_roi.index, rotation=45, ha="right")

    plt.ylim(0, genre_roi.max() * 1.15)


    plt.xlabel("Genre")
    plt.ylabel("Mean ROI")
    plt.title("Average ROI by Genre", fontweight="bold")
    #plt.grid(True, axis="y", alpha=0.3)
    plt.legend()

    plt.tight_layout()
    plt.savefig(f"{VISUAL_PATH}/roi_by_genre_bar.png", dpi=300)
    plt.close()


# 3. Popularity vs Rating
def plot_popularity_vs_rating(df):
    pdf = spark_to_pandas(df, ["popularity", "vote_average"]).dropna()

    plt.figure(figsize=(12, 6))

    plt.scatter(
        pdf["popularity"],
        pdf["vote_average"],
        alpha=0.6,
        edgecolors="k",
        linewidth=0.5,
        s=100,
        label="Movie Ratings"
    )

  

    plt.xlabel("Popularity")
    plt.ylabel("Average Rating")
    plt.title("Popularity vs Rating", fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.legend()

    plt.tight_layout()
    plt.savefig(f"{VISUAL_PATH}/popularity_vs_rating.png", dpi=300)
    plt.close()


# 4. Yearly Box Office Trends
def plot_yearly_trends(df):
    yearly_df = (
        df.withColumn("year", year(col("release_date")))
          .groupBy("year")
          .avg("revenue_musd")
          .orderBy("year")
    )

    pdf = yearly_df.toPandas()

    plt.figure(figsize=(12, 6))

    plt.plot(
        pdf["year"],
        pdf["avg(revenue_musd)"],
        marker="o",
        linewidth=2.5,
        label="Avg Annual Revenue"
    )

    plt.ylim(0, pdf["avg(revenue_musd)"].max() * 1.15)


    plt.xlabel("Year")
    plt.ylabel("Average Revenue (Million USD)")
    plt.title("Yearly Box Office Trends", fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.legend()

    plt.tight_layout()
    plt.savefig(f"{VISUAL_PATH}/yearly_trends.png", dpi=300)
    plt.close()


# 5. Franchise vs Standalone Comparison
def plot_franchise_vs_standalone():
    pdf = pd.read_parquet(f"{GOLD_PATH}/franchise_vs_standalone")

    color_map = {
        "Franchise": "skyblue",
        "Standalone": "orange"
    }

    colors = pdf["is_franchise"].map(color_map)

    metrics = {
        "mean_revenue": "Mean Revenue (Million USD)",
        "mean_popularity": "Mean Popularity",
        "mean_rating": "Mean Rating"
    }

    legend_handles = [
        mpatches.Patch(color="skyblue", label="Franchise"),
        mpatches.Patch(color="orange", label="Standalone")
    ]

    for metric, ylabel in metrics.items():
        plt.figure(figsize=(12, 7))

        bars = plt.bar(
            pdf["is_franchise"],
            pdf[metric],
            color=colors,
            edgecolor="black",
            linewidth=1.5,
            width=0.5
        )

        plt.ylim(0, pdf[metric].max() * 1.15)

        

        plt.xlabel("Movie Type")
        plt.ylabel(ylabel)
        plt.title(f"Franchise vs Standalone: {ylabel}", fontweight="bold")
        # plt.grid(True, axis="y", alpha=0.3)
        plt.legend(handles=legend_handles, title="Category")

        plt.tight_layout()
        plt.savefig(f"{VISUAL_PATH}/franchise_vs_standalone_{metric}.png", dpi=300)
        plt.close()


# ENTRY POINT
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

import os
import sys
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col
from src.utils.logger import get_logger
from src.utils.config import get_config

logger = get_logger(__name__)
config = get_config()

# PATHS - Use config with Docker fallback
SILVER_PATH = os.getenv("SILVER_PATH", config.paths.get("silver", "data/silver/movies_curated"))
GOLD_PATH = os.getenv("GOLD_PATH", config.paths.get("gold", "data/gold"))
VISUAL_PATH = os.getenv("VISUAL_PATH", config.paths.get("visualizations", "data/visualizations"))

# Handle Docker environment
if os.path.exists("/opt/app/data"):
    SILVER_PATH = "/opt/app/data/silver/movies_curated"
    GOLD_PATH = "/opt/app/data/gold"
    VISUAL_PATH = "/opt/app/data/visualizations"

DONE_DIR = os.path.join(VISUAL_PATH, ".done")


def find_unprocessed_silver_dates(silver_base: str) -> list:
    """Return sorted list of ingestion_date values present in silver but not yet visualized."""
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


def spark_to_pandas(df, cols):
    return df.select(*cols).toPandas()


def plot_revenue_vs_budget(df, visual_path):
    pdf = spark_to_pandas(df, ["budget_musd", "revenue_musd"]).dropna()

    plt.figure(figsize=(12, 6))
    plt.scatter(pdf["budget_musd"], pdf["revenue_musd"],
                alpha=0.6, edgecolors="k", linewidth=0.5, s=100, label="Movies")
    plt.xlabel("Budget (Million USD)")
    plt.ylabel("Revenue (Million USD)")
    plt.title("Revenue vs Budget", fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(visual_path, "revenue_vs_budget.png"), dpi=300)
    plt.close()


def plot_roi_by_genre(df, visual_path):
    pdf = spark_to_pandas(df, ["genres", "roi"]).dropna()

    pdf["genre"] = pdf["genres"].str.split("|")
    pdf = pdf.explode("genre")

    genre_roi = pdf.groupby("genre")["roi"].mean().sort_values(ascending=False)

    plt.figure(figsize=(12, 6))
    plt.bar(range(len(genre_roi)), genre_roi.values,
            edgecolor="black", linewidth=1.2, label="Genres")
    plt.xticks(range(len(genre_roi)), genre_roi.index, rotation=45, ha="right")
    plt.ylim(0, genre_roi.max() * 1.15)
    plt.xlabel("Genre")
    plt.ylabel("Mean ROI")
    plt.title("Average ROI by Genre", fontweight="bold")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(visual_path, "roi_by_genre_bar.png"), dpi=300)
    plt.close()


def plot_popularity_vs_rating(df, visual_path):
    pdf = spark_to_pandas(df, ["popularity", "vote_average"]).dropna()

    plt.figure(figsize=(12, 6))
    plt.scatter(pdf["popularity"], pdf["vote_average"],
                alpha=0.6, edgecolors="k", linewidth=0.5, s=100, label="Movie Ratings")
    plt.xlabel("Popularity")
    plt.ylabel("Average Rating")
    plt.title("Popularity vs Rating", fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(visual_path, "popularity_vs_rating.png"), dpi=300)
    plt.close()


def plot_yearly_trends(df, visual_path):
    yearly_df = (
        df.withColumn("year", year(col("release_date")))
          .groupBy("year")
          .avg("revenue_musd")
          .orderBy("year")
    )

    pdf = yearly_df.toPandas()

    plt.figure(figsize=(12, 6))
    plt.plot(pdf["year"], pdf["avg(revenue_musd)"],
             marker="o", linewidth=2.5, label="Avg Annual Revenue")
    plt.ylim(0, pdf["avg(revenue_musd)"].max() * 1.15)
    plt.xlabel("Year")
    plt.ylabel("Average Revenue (Million USD)")
    plt.title("Yearly Box Office Trends", fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(visual_path, "yearly_trends.png"), dpi=300)
    plt.close()


def plot_franchise_vs_standalone(gold_path, visual_path):
    pdf = pd.read_parquet(os.path.join(gold_path, "franchise_vs_standalone"))

    color_map = {"Franchise": "skyblue", "Standalone": "orange"}
    colors = pdf["is_franchise"].map(color_map)

    metrics = {
        "mean_revenue":    "Mean Revenue (Million USD)",
        "mean_popularity": "Mean Popularity",
        "mean_rating":     "Mean Rating",
    }

    legend_handles = [
        mpatches.Patch(color="skyblue", label="Franchise"),
        mpatches.Patch(color="orange",  label="Standalone"),
    ]

    for metric, ylabel in metrics.items():
        plt.figure(figsize=(12, 7))
        plt.bar(pdf["is_franchise"], pdf[metric],
                color=colors, edgecolor="black", linewidth=1.5, width=0.5)
        plt.ylim(0, pdf[metric].max() * 1.15)
        plt.xlabel("Movie Type")
        plt.ylabel(ylabel)
        plt.title(f"Franchise vs Standalone: {ylabel}", fontweight="bold")
        plt.legend(handles=legend_handles, title="Category")
        plt.tight_layout()
        plt.savefig(os.path.join(visual_path, f"franchise_vs_standalone_{metric}.png"), dpi=300)
        plt.close()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("MovieVisualization").getOrCreate()
    logger.info("Starting visualization job")

    dates = find_unprocessed_silver_dates(SILVER_PATH)
    if not dates:
        logger.info("No unprocessed silver partitions found. Nothing to do.")
        spark.stop()
        exit(0)

    logger.info(f"Found {len(dates)} unprocessed silver partition(s).")

    for date in dates:
        logger.info(f"Generating visualizations for ingestion_date={date}")
        try:
            silver_partition = os.path.join(SILVER_PATH, f"ingestion_date={date}")
            gold_partition = os.path.join(GOLD_PATH, f"ingestion_date={date}")
            visual_partition = os.path.join(VISUAL_PATH, f"ingestion_date={date}")
            os.makedirs(visual_partition, exist_ok=True)

            movies_df = spark.read.parquet(silver_partition)

            plot_revenue_vs_budget(movies_df, visual_partition)
            plot_roi_by_genre(movies_df, visual_partition)
            plot_popularity_vs_rating(movies_df, visual_partition)
            plot_yearly_trends(movies_df, visual_partition)
            plot_franchise_vs_standalone(gold_partition, visual_partition)

            mark_done(date)
            logger.info(f"Visualizations complete for ingestion_date={date}")
        except Exception as e:
            logger.error(f"Visualization job failed for ingestion_date={date}: {e}")

    spark.stop()
    logger.info("All visualizations generated successfully")

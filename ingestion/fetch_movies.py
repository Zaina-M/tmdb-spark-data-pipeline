import os
import json
import requests
import time
from datetime import date
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from src.utils.logger import get_logger
from requests.exceptions import RequestException


# Load Environment Variables

load_dotenv()


# Configuration
API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = "https://api.themoviedb.org/3/movie"

if not API_KEY:
    raise RuntimeError("TMDB_API_KEY not set")

MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397, 420818,
    24428, 168259, 99861, 284054, 12445, 181808, 330457,
    351286, 109445, 321612, 260513
]


# Bronze Storage (Partitioned by Ingestion Date)

INGESTION_DATE = date.today().isoformat()

BRONZE_BASE_PATH = f"data/bronze/movies/ingestion_date={INGESTION_DATE}"
RAW_JSON_PATH = f"{BRONZE_BASE_PATH}/movies_raw.json"
BRONZE_PARQUET_PATH = f"{BRONZE_BASE_PATH}/movies_raw.parquet"

REJECTED_BASE_PATH = f"data/bronze/rejected/ingestion_date={INGESTION_DATE}"
REJECTED_IDS_PATH = f"{REJECTED_BASE_PATH}/rejected_ids.json"

# Ensure directories exist
os.makedirs(BRONZE_BASE_PATH, exist_ok=True)
os.makedirs(REJECTED_BASE_PATH, exist_ok=True)


# Logger
logger = get_logger(__name__)


# Spark Session

spark = (
    SparkSession.builder
    .appName("Movie-API-Ingestion")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Validation Logic

def is_valid_movie(payload: dict) -> bool:

   # A movie is valid if: Required movie fields exist, Credits object exists, Payload is not a TMDb error response
    return (
        isinstance(payload, dict)
        and isinstance(payload.get("id"), int)
        and payload.get("title")
        and isinstance(payload.get("credits"), dict)
        and isinstance(payload["credits"].get("cast"), list)
        and isinstance(payload["credits"].get("crew"), list)
        and payload.get("success") is not False
    )
def fetch_movie_with_retries(
    movie_id: int,
    max_retries: int = 3,
    backoff: int = 2
):
    # Fetch a movie from TMDB with retries, rate-limit handling,and exponential backoff.

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                f"{BASE_URL}/{movie_id}",
                params={
                    "api_key": API_KEY,
                    "append_to_response": "credits"
                },
                timeout=10
            )

            # Success
            if response.status_code == 200:
                return response.json()

            # Rate limiting (TMDB)
            if response.status_code == 429:
                retry_after = int(
                    response.headers.get("Retry-After", backoff ** attempt)
                )
                logger.warning(
                    f"Rate limited (429) for movie {movie_id}. "
                    f"Sleeping {retry_after}s before retry"
                )
                time.sleep(retry_after)
                continue

            # Permanent failures → do NOT retry
            if response.status_code in (401, 403, 404):
                logger.warning(
                    f"Rejected movie ID {movie_id} | HTTP {response.status_code}"
                )
                return None

            # Other retryable failures
            logger.warning(
                f"Attempt {attempt}/{max_retries} failed "
                f"for movie {movie_id} | HTTP {response.status_code}"
            )

        except RequestException as e:
            logger.warning(
                f"Attempt {attempt}/{max_retries} failed "
                f"for movie {movie_id} | {e}"
            )

        # Exponential backoff
        if attempt < max_retries:
            time.sleep(backoff ** attempt)

    logger.error(f"Movie ID {movie_id} failed after {max_retries} retries")
    return None


# Fetch Movies (with Credits)

def fetch_movies(movie_ids):
    valid_movies = []
    rejected_ids = []

    for movie_id in movie_ids:
        payload = fetch_movie_with_retries(movie_id)

        if not payload:
            rejected_ids.append(movie_id)
            continue

        if not is_valid_movie(payload):
            logger.warning(
                f"Rejected movie ID {movie_id} | Invalid payload structure"
            )
            rejected_ids.append(movie_id)
            continue

        valid_movies.append(payload)
        logger.info(f"Ingested movie ID {payload['id']}")

    logger.info(f"Accepted movies: {len(valid_movies)}")
    logger.info(f"Rejected movie IDs: {rejected_ids}")

    return valid_movies, rejected_ids



# Main Execution

if __name__ == "__main__":
    logger.info("Starting movie ingestion job")

    movies_data, rejected_ids = fetch_movies(MOVIE_IDS)


    if not movies_data:
        logger.error("No valid movie data ingested. Job aborted.")
        spark.stop()
        exit(1)

    # Save raw JSON (Bronze - immutable)
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(movies_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Raw JSON written to {RAW_JSON_PATH}")

    # Convert JSON -> Parquet (Spark-native bronze)
    df_raw = spark.createDataFrame(movies_data)
    # Save rejected IDs (Bronze diagnostics)
    with open(REJECTED_IDS_PATH, "w", encoding="utf-8") as f:
      json.dump(rejected_ids, f, indent=2)

    logger.info(f"Rejected IDs written to {REJECTED_IDS_PATH}")



    df_raw.write.mode("overwrite").parquet(BRONZE_PARQUET_PATH)

    logger.info(f"Bronze Parquet written to {BRONZE_PARQUET_PATH}")
    logger.info("Movie ingestion job completed successfully")
    

    spark.stop()

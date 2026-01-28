import os
import json
import requests
import time
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from dotenv import load_dotenv
from src.utils.logger import get_logger
from src.utils.config import get_config
from src.schemas import MovieSchema
from requests.exceptions import RequestException


# Load Environment Variables
load_dotenv()

# Load Configuration
config = get_config()

# Configuration
API_KEY = os.getenv("TMDB_API_KEY")
BASE_URL = config.api.get("base_url", "https://api.themoviedb.org/3/movie")

if not API_KEY:
    raise RuntimeError("TMDB_API_KEY not set")

# Get movie IDs from config instead of hardcoding
MOVIE_IDS = config.get_movie_ids()


# Bronze Storage (Partitioned by Ingestion Date + Timestamp for multiple runs)

INGESTION_DATE = date.today().isoformat()
RUN_TIMESTAMP = datetime.now().strftime("%H%M%S")  # e.g., "143025" for 2:30:25 PM

BRONZE_BASE_PATH = f"data/bronze/movies/ingestion_date={INGESTION_DATE}"
RAW_JSON_PATH = f"{BRONZE_BASE_PATH}/movies_raw_{RUN_TIMESTAMP}.json"
BRONZE_PARQUET_PATH = f"{BRONZE_BASE_PATH}/movies_raw_{RUN_TIMESTAMP}.parquet"

REJECTED_BASE_PATH = f"data/bronze/rejected/ingestion_date={INGESTION_DATE}"
REJECTED_IDS_PATH = f"{REJECTED_BASE_PATH}/rejected_ids_{RUN_TIMESTAMP}.json"

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
    """
    A movie is valid if:
    - Required movie fields exist (id, title)
    - Credits object exists with cast and crew lists
    - Payload is not a TMDb error response
    """
    if not isinstance(payload, dict):
        return False
    
    if not isinstance(payload.get("id"), int):
        return False
    
    if not payload.get("title"):
        return False
    
    credits = payload.get("credits")
    if not isinstance(credits, dict):
        return False
    
    if not isinstance(credits.get("cast"), list):
        return False
    
    if not isinstance(credits.get("crew"), list):
        return False
    
    # Check for TMDB error response
    if payload.get("success") is False:
        return False
    
    return True

# Fetch Movies (with Credits - Concurrent)

class ConcurrentMovieIngestion:

    
    def __init__(self, api_key: str, base_url: str, max_workers: int = 10):
        self.api_key = api_key
        self.base_url = base_url
        self.max_workers = max_workers
        self.session = requests.Session()
        # Configure session with connection pooling
        adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
        self.session.mount('http://', adapter) # Apply this adapter to all HTTPS requests
        self.session.mount('https://', adapter)
    
    def fetch_single_movie(self, movie_id: int, max_retries: int = 3, backoff: int = 2) -> dict:
      
        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.get(
                    f"{self.base_url}/{movie_id}",
                    params={
                        "api_key": self.api_key,
                        "append_to_response": "credits"
                    },
                    timeout=10
                )

                
                # Success
                if response.status_code == 200:
                    logger.info(f" Fetched movie {movie_id}")
                    return {
                        "success": True,
                        "movie_id": movie_id,
                        "data": response.json()
                    }
                
                # Rate limiting (TMDB)
                if response.status_code == 429:
                    retry_after = int(
                        response.headers.get("Retry-After", backoff ** attempt)
                    )
                    logger.warning(
                        f" Rate limited (429) for movie {movie_id}. "
                        f"Waiting {retry_after}s..."
                    )
                    time.sleep(retry_after)
                    continue
                
                # Permanent failures → do NOT retry
                if response.status_code in (401, 403, 404):
                    logger.warning(
                        f" Movie {movie_id} rejected | HTTP {response.status_code}"
                    )
                    return {
                        "success": False,
                        "movie_id": movie_id,
                        "error": f"HTTP_{response.status_code}"
                    }
                
                # Other retryable failures
                logger.warning(
                    f" Attempt {attempt}/{max_retries} failed "
                    f"for movie {movie_id} | HTTP {response.status_code}"
                )
            
            except requests.exceptions.Timeout:
                logger.warning(
                    f" Timeout for movie {movie_id}, attempt {attempt}/{max_retries}"
                )
                if attempt < max_retries:
                    time.sleep(backoff ** attempt)
                    continue
            
            except RequestException as e:
                logger.warning(
                    f" Error fetching {movie_id} (attempt {attempt}/{max_retries}): {e}"
                )
                if attempt < max_retries:
                    time.sleep(backoff ** attempt)
                    continue
            
            except Exception as e:
                logger.error(f"Unexpected error for movie {movie_id}: {e}")
                return {
                    "success": False,
                    "movie_id": movie_id,
                    "error": f"UNEXPECTED_{type(e).__name__}"
                }
            
            # Exponential backoff between retries
            if attempt < max_retries:
                time.sleep(backoff ** attempt)
        
        logger.error(f"    Movie {movie_id} failed after {max_retries} retries")
        return {
            "success": False,
            "movie_id": movie_id,
            "error": "MAX_RETRIES_EXCEEDED"
        }
    
    def fetch_all_concurrent(self, movie_ids: list) -> tuple:
        
        valid_movies = []
        rejected_ids = []
        
        start_time = time.time()
        logger.info(f" Starting concurrent ingestion of {len(movie_ids)} movies")
        logger.info(f"   Using {self.max_workers} workers")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            futures = {
                executor.submit(self.fetch_single_movie, movie_id): movie_id
                for movie_id in movie_ids
            }
            
            # Process results as they complete
            completed = 0
            for future in as_completed(futures):
                completed += 1
                movie_id = futures[future]
                
                try:
                    result = future.result()
                    
                    if result["success"]:
                        payload = result["data"]
                        
                        # Validate movie structure
                        if is_valid_movie(payload):
                            valid_movies.append(payload)
                        else:
                            logger.warning(
                                f" Movie {movie_id} has invalid structure"
                            )
                            rejected_ids.append(movie_id)
                    else:
                        rejected_ids.append(movie_id)
                        logger.debug(f"Rejected {movie_id}: {result['error']}")
                
                except Exception as e:
                    logger.error(f"Error processing result for {movie_id}: {e}")
                
                # Progress indicator
                if completed % max(1, len(movie_ids) // 5) == 0:
                    progress = (completed / len(movie_ids)) * 100
                    elapsed = time.time() - start_time
                    logger.info(f"   Progress: {completed}/{len(movie_ids)} ({progress:.0f}%) - {elapsed:.1f}s elapsed")
        
        elapsed = time.time() - start_time
        logger.info(f"\n Concurrent Ingestion Summary:")
        logger.info(f"    Valid movies: {len(valid_movies)}")
        logger.info(f"    Rejected: {len(rejected_ids)}")
        logger.info(f"    Total time: {elapsed:.2f}s")
        logger.info(f"    Throughput: {len(movie_ids)/elapsed:.2f} movies/sec")
        logger.info(f"    Speedup: ~{120/elapsed:.1f}x vs sequential\n")
        
        return valid_movies, rejected_ids



# Main Execution

if __name__ == "__main__":
    logger.info("Starting concurrent movie ingestion job")

    # Uses concurrent ingestion instead of sequential
    ingestion = ConcurrentMovieIngestion(
        api_key=API_KEY,
        base_url=BASE_URL,
        max_workers=10  # Adjust based on API rate limits
    )
    
    movies_data, rejected_ids = ingestion.fetch_all_concurrent(MOVIE_IDS)


    if not movies_data:
        logger.error("No valid movie data ingested. Job aborted.")
        spark.stop()
        exit(1)

    # Save raw JSON (Bronze - immutable)
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(movies_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Raw JSON written to {RAW_JSON_PATH}")

    # Convert JSON -> Parquet with schema validation
    df_raw = spark.createDataFrame(movies_data, schema=MovieSchema.BRONZE_SCHEMA)
    
    # Add audit columns for tracking when data was ingested
    ingestion_ts = datetime.now().isoformat()
    df_raw = df_raw.withColumn("ingestion_timestamp", lit(ingestion_ts))
    df_raw = df_raw.withColumn("run_id", lit(RUN_TIMESTAMP))
    
    # Save rejected IDs (Bronze diagnostics)
    with open(REJECTED_IDS_PATH, "w", encoding="utf-8") as f:
        json.dump(rejected_ids, f, indent=2)

    logger.info(f"Rejected IDs written to {REJECTED_IDS_PATH}")

    # Append mode ensures Bronze layer is immutable (no accidental overwrites)
    # Each run writes to a unique timestamped path anyway, but append is safer
    df_raw.write.mode("append").parquet(BRONZE_PARQUET_PATH)

    logger.info(f"Bronze Parquet written to {BRONZE_PARQUET_PATH}")
    logger.info(f"Schema validation:  Passed" if MovieSchema.validate_schema(df_raw, MovieSchema.BRONZE_SCHEMA) else "  Schema mismatch")
    logger.info("Movie ingestion job completed successfully")
    

    spark.stop()

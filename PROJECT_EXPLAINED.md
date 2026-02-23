# Movie Data Pipeline - Line by Line Explanation

This document explains every part of the movie data pipeline in simple terms, starting with the ingestion module and moving through each stage.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Ingestion Module (fetch_movies.py)](#ingestion-module-fetch_moviespy)
3. [Transformation Module (clean_transform.py)](#transformation-module-clean_transformpy)
4. [Analytics Module (kpis.py)](#analytics-module-kpispy)
5. [Visualization Module (visualize.py)](#visualization-module-visualizepy)
6. [Supporting Files](#supporting-files)

---

## Project Overview

This pipeline follows the **Medallion Architecture** - a data design pattern with three layers:

| Layer | What It Does |
|-------|--------------|
| **Bronze** | Raw data exactly as it comes from the source (API) |
| **Silver** | Cleaned, validated, and transformed data |
| **Gold** | Business metrics and KPIs ready for analysis |

**Data Flow:**
```
TMDB API → Bronze (Raw JSON) → Silver (Clean Data) → Gold (KPIs) → Visualizations
```

---

## Ingestion Module (fetch_movies.py)

This is the **first step** in the pipeline. It fetches movie data from the TMDB (The Movie Database) API.

### Imports Section (Lines 1-14)

```python
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
```

**What each import does:**

| Import | Simple Explanation |
|--------|-------------------|
| `os` | Lets you work with files and folders on your computer |
| `json` | Lets you read and write JSON files (like a dictionary saved to a file) |
| `requests` | Makes HTTP calls to websites and APIs (like visiting a webpage) |
| `time` | Provides functions to pause/wait between operations |
| `datetime` | Helps you work with dates and times |
| `ThreadPoolExecutor` | Runs multiple tasks at the same time (like having multiple workers) |
| `SparkSession` | Creates a connection to Apache Spark for big data processing |
| `lit` | Creates a column with the same value for every row |
| `load_dotenv` | Loads secret values (like API keys) from a `.env` file |
| `get_logger` | Custom function to create a logger for tracking what happens |
| `get_config` | Custom function to load settings from config.yaml |
| `MovieSchema` | Defines the expected structure of movie data |
| `RequestException` | An error type that occurs when web requests fail |

---

### Loading Configuration (Lines 16-31)

```python
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
```

**Line-by-line explanation:**

| Line | What It Does |
|------|--------------|
| `load_dotenv()` | Reads the `.env` file and loads API keys into memory |
| `config = get_config()` | Loads all settings from `config/config.yaml` |
| `API_KEY = os.getenv("TMDB_API_KEY")` | Gets the secret API key needed to access TMDB |
| `BASE_URL = ...` | Sets the web address of the TMDB API |
| `if not API_KEY: raise RuntimeError(...)` | Crashes the program if no API key is found (we can't continue without it!) |
| `MOVIE_IDS = config.get_movie_ids()` | Gets the list of movie IDs we want to fetch from the config file |

---

### File Path Setup (Lines 34-49)

```python
INGESTION_DATE = date.today().isoformat()
RUN_TIMESTAMP = datetime.now().strftime("%H%M%S")

BRONZE_BASE_PATH = f"data/bronze/movies/ingestion_date={INGESTION_DATE}"
RAW_JSON_PATH = f"{BRONZE_BASE_PATH}/movies_raw_{RUN_TIMESTAMP}.json"
BRONZE_PARQUET_PATH = f"{BRONZE_BASE_PATH}/movies_raw_{RUN_TIMESTAMP}.parquet"

REJECTED_BASE_PATH = f"data/bronze/rejected/ingestion_date={INGESTION_DATE}"
REJECTED_IDS_PATH = f"{REJECTED_BASE_PATH}/rejected_ids_{RUN_TIMESTAMP}.json"

os.makedirs(BRONZE_BASE_PATH, exist_ok=True)
os.makedirs(REJECTED_BASE_PATH, exist_ok=True)
```

**What's happening:**

| Variable | Purpose | Example Value |
|----------|---------|---------------|
| `INGESTION_DATE` | Today's date as a string | `"2026-01-28"` |
| `RUN_TIMESTAMP` | Current time (hour/min/sec) | `"143025"` (2:30:25 PM) |
| `BRONZE_BASE_PATH` | Folder to store raw movie data | `"data/bronze/movies/ingestion_date=2026-01-28"` |
| `RAW_JSON_PATH` | Where to save the raw JSON file | Includes timestamp so multiple runs don't overwrite |
| `REJECTED_IDS_PATH` | Where to save IDs of movies that failed | Movies we couldn't fetch |

The `os.makedirs(..., exist_ok=True)` creates the folders if they don't exist yet.

---

### Logger and Spark Setup (Lines 52-63)

```python
logger = get_logger(__name__)

spark = (
    SparkSession.builder
    .appName("Movie-API-Ingestion")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
```

**Simple explanation:**

| Code | What It Does |
|------|--------------|
| `logger = get_logger(__name__)` | Creates a logger that prints messages with timestamps |
| `SparkSession.builder` | Starts building a Spark session (like starting up a powerful data engine) |
| `.appName("Movie-API-Ingestion")` | Names this Spark job so you can identify it |
| `.config("spark.sql.shuffle.partitions", "4")` | Sets how data is split up (4 parts = smaller datasets) |
| `.getOrCreate()` | Either gets an existing Spark session or creates a new one |
| `.setLogLevel("WARN")` | Only shows warning messages (not every little detail) |

---

### Validation Function (Lines 66-92)

```python
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
    
    if payload.get("success") is False:
        return False
    
    return True
```

**This function checks if a movie's data is "good enough" to use:**

| Check | What It Means |
|-------|---------------|
| `isinstance(payload, dict)` | Is the data a dictionary (key-value pairs)? |
| `isinstance(payload.get("id"), int)` | Does it have a numeric ID? |
| `payload.get("title")` | Does it have a title? |
| `isinstance(credits, dict)` | Does it have a credits section? |
| `credits.get("cast")` is a list | Does it have a list of actors? |
| `credits.get("crew")` is a list | Does it have a list of crew members? |
| `payload.get("success") is False` | Is this an error response from TMDB? |

If **any check fails**, the movie is rejected. Only valid movies move forward.

---

### ConcurrentMovieIngestion Class (Lines 95-200)

This is the main class that fetches movies from the API.

#### Constructor (Lines 97-106)

```python
class ConcurrentMovieIngestion:
    def __init__(self, api_key: str, base_url: str, max_workers: int = 10):
        self.api_key = api_key
        self.base_url = base_url
        self.max_workers = max_workers
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
```

**What's happening:**

| Code | Simple Explanation |
|------|-------------------|
| `__init__` | This runs when you create a new `ConcurrentMovieIngestion` object |
| `self.api_key` | Stores the secret API key |
| `self.max_workers = 10` | Maximum 10 simultaneous requests (like 10 workers at once) |
| `requests.Session()` | Creates a reusable connection (faster than making a new one each time) |
| `HTTPAdapter` | Configures the connection pool (keeps connections open for reuse) |
| `.mount(...)` | Applies this adapter to all HTTP and HTTPS requests |

---

#### fetch_single_movie Method (Lines 108-175)

```python
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
```

**This function fetches ONE movie from the API:**

| Parameter | What It Means |
|-----------|---------------|
| `movie_id` | The unique ID of the movie to fetch (e.g., 550 for Fight Club) |
| `max_retries = 3` | Try up to 3 times if it fails |
| `backoff = 2` | Wait longer between each retry (exponential backoff) |

**The API call:**
- `self.session.get(...)` - Makes an HTTP GET request
- `f"{self.base_url}/{movie_id}"` - URL like `https://api.themoviedb.org/3/movie/550`
- `params` - Extra info sent with the request:
  - `api_key` - Your secret key
  - `append_to_response: "credits"` - Also get cast/crew info in one call!
- `timeout=10` - Give up if no response after 10 seconds

---

#### Handling Different Response Types (Lines 118-165)

```python
# Success
if response.status_code == 200:
    logger.info(f" Fetched movie {movie_id}")
    return {"success": True, "movie_id": movie_id, "data": response.json()}

# Rate limiting (TMDB)
if response.status_code == 429:
    retry_after = int(response.headers.get("Retry-After", backoff ** attempt))
    logger.warning(f" Rate limited (429) for movie {movie_id}. Waiting {retry_after}s...")
    time.sleep(retry_after)
    continue

# Permanent failures → do NOT retry
if response.status_code in (401, 403, 404):
    logger.warning(f" Movie {movie_id} rejected | HTTP {response.status_code}")
    return {"success": False, "movie_id": movie_id, "error": f"HTTP_{response.status_code}"}
```

**HTTP status codes explained:**

| Status Code | Meaning | What We Do |
|-------------|---------|------------|
| `200` | Success! | Save the data and return it |
| `429` | Too many requests (rate limited) | Wait and try again |
| `401` | Unauthorized (bad API key) | Give up - can't fix this |
| `403` | Forbidden (no access) | Give up - movie is restricted |
| `404` | Not found (movie doesn't exist) | Give up - movie ID is wrong |

---

#### fetch_all_concurrent Method (Lines 177-230)

```python
def fetch_all_concurrent(self, movie_ids: list) -> tuple:
    valid_movies = []
    rejected_ids = []
    
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        futures = {
            executor.submit(self.fetch_single_movie, movie_id): movie_id
            for movie_id in movie_ids
        }
        
        for future in as_completed(futures):
            result = future.result()
            
            if result["success"]:
                payload = result["data"]
                if is_valid_movie(payload):
                    valid_movies.append(payload)
                else:
                    rejected_ids.append(movie_id)
            else:
                rejected_ids.append(movie_id)
    
    return valid_movies, rejected_ids
```

**This is the "magic" that fetches all movies at once:**

| Concept | Simple Explanation |
|---------|-------------------|
| `ThreadPoolExecutor` | Creates a pool of workers (like hiring 10 people to fetch movies) |
| `executor.submit(...)` | Tells a worker to fetch one movie |
| `futures` | A dictionary of "promises" - results that will come later |
| `as_completed(futures)` | Gets results as soon as each one finishes (not waiting for all) |
| `valid_movies` | List of successfully fetched and validated movies |
| `rejected_ids` | List of movie IDs that failed |

**Why concurrent?** 
- Sequential: Fetch movie 1, wait, fetch movie 2, wait... (SLOW)
- Concurrent: Fetch movies 1, 2, 3, 4, 5 all at once! (FAST - about 10x faster)

---

### Main Execution Block (Lines 233-280)

```python
if __name__ == "__main__":
    logger.info("Starting concurrent movie ingestion job")

    ingestion = ConcurrentMovieIngestion(
        api_key=API_KEY,
        base_url=BASE_URL,
        max_workers=10
    )
    
    movies_data, rejected_ids = ingestion.fetch_all_concurrent(MOVIE_IDS)

    if not movies_data:
        logger.error("No valid movie data ingested. Job aborted.")
        spark.stop()
        exit(1)

    # Save raw JSON (Bronze - immutable)
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(movies_data, f, ensure_ascii=False, indent=2)
```

**Step by step:**

| Step | What Happens |
|------|--------------|
| 1 | Create the ingestion object with API key and settings |
| 2 | Fetch all movies concurrently |
| 3 | Check if we got any valid movies (if not, stop!) |
| 4 | Save raw JSON to the Bronze folder |

---

#### Saving to Parquet (Lines 264-280)

```python
df_raw = spark.createDataFrame(movies_data, schema=MovieSchema.BRONZE_SCHEMA)

ingestion_ts = datetime.now().isoformat()
df_raw = df_raw.withColumn("ingestion_timestamp", lit(ingestion_ts))
df_raw = df_raw.withColumn("run_id", lit(RUN_TIMESTAMP))

with open(REJECTED_IDS_PATH, "w", encoding="utf-8") as f:
    json.dump(rejected_ids, f, indent=2)

df_raw.write.mode("append").parquet(BRONZE_PARQUET_PATH)
```

**What's happening:**

| Code | Explanation |
|------|-------------|
| `spark.createDataFrame(...)` | Converts the Python list to a Spark DataFrame (like a table) |
| `schema=MovieSchema.BRONZE_SCHEMA` | Enforces the expected data structure |
| `.withColumn("ingestion_timestamp", ...)` | Adds a column showing WHEN the data was fetched |
| `.withColumn("run_id", ...)` | Adds a column to identify this specific run |
| `.write.mode("append").parquet(...)` | Saves the data as a Parquet file (compressed, efficient format) |

---

## Transformation Module (clean_transform.py)

This is **Step 2** - it takes the raw Bronze data and cleans it up.

### Imports and Setup (Lines 1-16)

```python
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
```

**Key functions imported:**

| Function | What It Does |
|----------|--------------|
| `col` | Refers to a column by name |
| `when` | Like an if-else statement for columns |
| `size` | Counts items in an array |
| `concat_ws` | Joins strings with a separator (like "Action\|Comedy\|Drama") |
| `to_date` | Converts a string to a date |
| `filter` | Filters items in an array |
| `transform` | Applies a function to each item in an array |

---

### Reading Bronze Data (Lines 29-35)

```python
df = spark.read.option("multiline", "true").json(BRONZE_BASE_PATH)

df.cache()
initial_count = df.count()
logger.info(f"Initial row count: {initial_count}")
```

| Code | What It Does |
|------|--------------|
| `spark.read.json(...)` | Reads all JSON files from the Bronze folder |
| `.option("multiline", "true")` | Handles JSON that spans multiple lines |
| `df.cache()` | Stores the data in memory for faster access |
| `df.count()` | Counts total rows (we'll compare this later) |

---

### Filtering Released Movies (Lines 37-39)

```python
if "status" in df.columns:
    df = df.filter(col("status") == "Released")
```

**Simple:** Only keep movies that have been released (not "In Production" or "Rumored").

---

### Dropping Irrelevant Columns (Lines 42-49)

```python
drop_cols = [
    "adult", "imdb_id", "original_title",
    "video", "homepage", "status"
]

df = df.drop(*[c for c in drop_cols if c in df.columns])
```

**These columns are removed because:**

| Column | Why We Drop It |
|--------|----------------|
| `adult` | We already filter for non-adult content |
| `imdb_id` | We don't need the IMDB reference |
| `original_title` | Duplicate of `title` in most cases |
| `video` | Not useful for analysis |
| `homepage` | URLs aren't needed for metrics |
| `status` | Already filtered for "Released" |

---

### Flattening Nested JSON (Lines 52-63)

```python
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
```

**What's happening:**

The raw data has nested structures like:
```json
"genres": [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}]
```

We flatten it to:
```
"genres": "Action|Adventure"
```

| Original Format | Flattened Format |
|-----------------|------------------|
| `[{name: "Action"}, {name: "Adventure"}]` | `"Action\|Adventure"` |
| `{id: 10, name: "Marvel Collection"}` | `"Marvel Collection"` |

**Why?** Flat strings are easier to query and analyze!

---

### Extracting Cast and Director (Lines 65-79)

```python
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
```

**What each transformation does:**

| New Column | How It's Created |
|------------|------------------|
| `cast` | All actor names joined with `\|` (e.g., "Brad Pitt\|Edward Norton") |
| `cast_size` | How many actors are in the movie |
| `director` | Finds the person with job="Director" and takes their name |
| `crew_size` | How many crew members total |

The `filter(..., lambda x: x["job"] == "Director")` finds all crew members who are directors, then `[0]` takes the first one.

---

### Type Casting (Lines 82-97)

```python
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
```

**Why type casting matters:**

| Column | Before | After |
|--------|--------|-------|
| `budget` | String "150000000" | Number 150000000.0 |
| `release_date` | String "2019-04-24" | Date object |
| `runtime` | String "181" | Integer 181 |

Proper types allow math operations and proper sorting!

---

### Handling Unrealistic Values (Lines 100-122)

```python
df = (
    df
    .withColumn("budget", when(col("budget") <= 0, None).otherwise(col("budget")))
    .withColumn("revenue", when(col("revenue") <= 0, None).otherwise(col("revenue")))
    .withColumn("runtime", when(col("runtime") <= 0, None).otherwise(col("runtime")))
)

df = (
    df
    .withColumn("budget_musd", col("budget") / 1_000_000)
    .withColumn("revenue_musd", col("revenue") / 1_000_000)
    .drop("budget", "revenue")
)

df = df.withColumn(
    "vote_average",
    when(col("vote_count") == 0, None).otherwise(col("vote_average"))
)
```

**Data cleaning rules:**

| Rule | Why |
|------|-----|
| Budget ≤ 0 → `null` | A $0 budget is usually missing data, not actually free |
| Revenue ≤ 0 → `null` | Same - unreported, not truly zero |
| Convert to millions | 150000000 → 150.0 (easier to read!) |
| vote_count = 0 → rating is `null` | A rating with 0 votes is meaningless |

---

### Removing Duplicates and Invalid Rows (Lines 125-133)

```python
df = df.dropDuplicates(["id"])
df = df.dropna(subset=["id", "title"])

non_null_expr = sum(col(c).isNotNull().cast("int") for c in df.columns)
df = df.filter(non_null_expr >= 10)
```

| Cleaning Step | What It Does |
|---------------|--------------|
| `dropDuplicates(["id"])` | Remove duplicate movies (same ID) |
| `dropna(subset=["id", "title"])` | Remove rows without an ID or title |
| `filter(non_null_expr >= 10)` | Keep only rows with at least 10 non-null columns (remove empty rows) |

---

### Calculating ROI (Lines 135-140)

```python
df = df.withColumn(
    "roi",
    when(col("budget_musd") > 0, col("revenue_musd") / col("budget_musd"))
)
```

**ROI = Return on Investment**

Formula: `ROI = Revenue / Budget`

Example:
- Budget: $100 million
- Revenue: $500 million
- ROI: 5.0 (made 5x their investment!)

Only calculated when budget > 0 (can't divide by zero!).

---

### Writing to Silver Layer (Lines 155-166)

```python
final_count = df.count()
df.write.mode("overwrite").parquet(SILVER_PATH)
df.unpersist()

logger.info(f"Curated dataset written to {SILVER_PATH}")
logger.info(f"Final row count: {final_count}")
logger.info(f"Rows dropped: {initial_count - final_count}")
```

| Action | What It Does |
|--------|--------------|
| `df.write.mode("overwrite").parquet(...)` | Save cleaned data as Parquet (overwrites previous version) |
| `df.unpersist()` | Free up the memory we used for caching |
| Logging the counts | Shows how many rows survived the cleaning |

---

## Analytics Module (kpis.py)

This is **Step 3** - calculating business metrics (KPIs = Key Performance Indicators).

### The prepare_kpis Function (Lines 18-27)

```python
def prepare_kpis(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("profit_musd", col("revenue_musd") - col("budget_musd"))
        .withColumn(
            "roi",
            when(col("budget_musd") >= 10, col("revenue_musd") / col("budget_musd"))
        )
    )
```

**New columns added:**

| Column | Formula | Example |
|--------|---------|---------|
| `profit_musd` | Revenue - Budget | $500M - $100M = $400M profit |
| `roi` | Revenue / Budget (only if budget ≥ $10M) | $500M / $100M = 5.0 |

**Why require budget ≥ $10M for ROI?** Low-budget films can have misleadingly high ROI (a $10,000 film making $100,000 = 10x ROI, but that's not impressive).

---

### The rank_movies Function (Lines 30-45)

```python
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
```

**This is a generic function to find "top N" movies:**

| Parameter | What It Does |
|-----------|--------------|
| `metric_col` | Which column to rank by (e.g., "revenue_musd") |
| `order` | "desc" = highest first, "asc" = lowest first |
| `top_n` | How many to return (default 5) |
| `filter_expr` | Optional filter (e.g., only movies with 10+ votes) |

---

### Movie KPIs (Lines 48-80)

```python
def run_movie_kpis(df: DataFrame, gold_path: str):
    kpis = {
        "highest_revenue": rank_movies(df, "revenue_musd", "desc", 5),
        "highest_budget": rank_movies(df, "budget_musd", "desc", 5),
        "highest_profit": rank_movies(df, "profit_musd", "desc", 5),
        "lowest_profit": rank_movies(df, "profit_musd", "asc", 5),
        "highest_roi": rank_movies(df, "roi", "desc", 5, col("budget_musd") >= 10),
        "lowest_roi": rank_movies(df, "roi", "asc", 5, col("budget_musd") >= 10),
        "most_voted": rank_movies(df, "vote_count", "desc", 5),
        "highest_rated": rank_movies(df, "vote_average", "desc", 5, col("vote_count") >= 10),
        "lowest_rated": rank_movies(df, "vote_average", "asc", 5, col("vote_count") >= 10),
        "most_popular": rank_movies(df, "popularity", "desc", 5)
    }
```

**Each KPI explained:**

| KPI Name | What It Finds |
|----------|---------------|
| `highest_revenue` | Top 5 movies that made the most money |
| `highest_budget` | Top 5 most expensive movies to make |
| `highest_profit` | Top 5 movies with biggest profit (revenue - budget) |
| `lowest_profit` | Top 5 biggest money-losers (or smallest profit) |
| `highest_roi` | Top 5 best return on investment |
| `lowest_roi` | Top 5 worst return on investment |
| `most_voted` | Top 5 movies with most user votes |
| `highest_rated` | Top 5 best-rated movies (with at least 10 votes) |
| `lowest_rated` | Top 5 worst-rated movies (with at least 10 votes) |
| `most_popular` | Top 5 by TMDB popularity score |

---

### Advanced Search Queries (Lines 83-112)

```python
def run_search_queries(df: DataFrame, gold_path: str):
    # Search 1: Best-rated Sci-Fi Action movies starring Bruce Willis
    search_1 = (
        df.filter(
            (col("genres").contains("Science Fiction")) &
            (col("genres").contains("Action")) &
            (col("cast").contains("Bruce Willis")) &
            (col("vote_count") >= 10)
        )
        .orderBy(desc("vote_average"))
    )

    # Search 2: Uma Thurman movies directed by Quentin Tarantino
    search_2 = (
        df.filter(
            (col("cast").contains("Uma Thurman")) &
            (col("director") == "Quentin Tarantino")
        )
        .orderBy(asc("runtime"))
    )
```

**Example use cases:**
- "What are Bruce Willis's best sci-fi action movies?"
- "What Tarantino films has Uma Thurman been in?"

---

### Franchise vs Standalone Analysis (Lines 115-135)

```python
def franchise_vs_standalone(df: DataFrame, gold_path: str):
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
```

**This answers:** "Do franchise movies (sequels, series) perform better than standalone films?"

| Metric | What It Measures |
|--------|------------------|
| `mean_revenue` | Average revenue for franchises vs standalones |
| `median_roi` | Middle ROI value (less affected by outliers) |
| `mean_budget` | Average budget |
| `mean_popularity` | Average TMDB popularity score |
| `mean_rating` | Average user rating |

---

### Top Franchises (Lines 138-155)

```python
def top_franchises(df: DataFrame, gold_path: str):
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
```

**Finds the most successful movie franchises** (like Marvel, Star Wars, Harry Potter).

---

### Top Directors (Lines 158-172)

```python
def top_directors(df: DataFrame, gold_path: str):
    stats = (
        df.groupBy("director")
        .agg(
            expr("count(*)").alias("num_movies"),
            round(expr("sum(revenue_musd)"), 2).alias("total_revenue"),
            round(expr("avg(vote_average)"), 2).alias("mean_rating")
        )
        .orderBy(desc("total_revenue"))
    )
```

**Finds directors whose movies make the most money total.**

---

## Visualization Module (visualize.py)

This is **Step 4** - creating charts and graphs.

### Helper Function (Lines 18-20)

```python
def spark_to_pandas(df, cols):
    return df.select(*cols).toPandas()
```

**Converts Spark DataFrame to Pandas** (matplotlib works with Pandas, not Spark).

---

### Revenue vs Budget Chart (Lines 23-47)

```python
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
    plt.savefig(f"{VISUAL_PATH}/revenue_vs_budget.png", dpi=300)
```

**Creates a scatter plot** showing the relationship between how much movies cost and how much they earn.

---

### ROI by Genre (Lines 50-82)

```python
def plot_roi_by_genre(df):
    pdf = spark_to_pandas(df, ["genres", "roi"]).dropna()
    pdf["genre"] = pdf["genres"].str.split("|")
    pdf = pdf.explode("genre")

    genre_roi = pdf.groupby("genre")["roi"].mean().sort_values(ascending=False)

    plt.bar(range(len(genre_roi)), genre_roi.values)
    plt.xticks(range(len(genre_roi)), genre_roi.index, rotation=45)
    plt.savefig(f"{VISUAL_PATH}/roi_by_genre_bar.png", dpi=300)
```

**Shows which genres have the best return on investment.**

The `explode("genre")` is important - it turns one row with "Action|Adventure" into TWO rows (one for Action, one for Adventure).

---

### Yearly Trends (Lines 118-148)

```python
def plot_yearly_trends(df):
    yearly_df = (
        df.withColumn("year", year(col("release_date")))
          .groupBy("year")
          .avg("revenue_musd")
          .orderBy("year")
    )
    pdf = yearly_df.toPandas()
    plt.plot(pdf["year"], pdf["avg(revenue_musd)"], marker="o")
    plt.savefig(f"{VISUAL_PATH}/yearly_trends.png", dpi=300)
```

**Shows how average movie revenue has changed over the years.**

---

### Franchise vs Standalone Comparison (Lines 151-198)

```python
def plot_franchise_vs_standalone():
    pdf = pd.read_parquet(f"{GOLD_PATH}/franchise_vs_standalone")
    
    metrics = {
        "mean_revenue": "Mean Revenue (Million USD)",
        "mean_popularity": "Mean Popularity",
        "mean_rating": "Mean Rating"
    }

    for metric, ylabel in metrics.items():
        plt.bar(pdf["is_franchise"], pdf[metric], color=colors)
        plt.savefig(f"{VISUAL_PATH}/franchise_vs_standalone_{metric}.png", dpi=300)
```

**Creates bar charts comparing franchise movies to standalone movies.**

---

## Supporting Files

### Logger (src/utils/logger.py)

```python
def get_logger(name: str):
    logger = logging.getLogger(name)
    
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger
```

**Creates a logger that prints messages like:**
```
2026-01-28 14:30:25 | INFO | ingestion.fetch_movies | Starting movie ingestion
```

---

### Schemas (src/schemas.py)

Defines the **exact structure** expected for data:

**Bronze Schema** = Raw API response structure (nested JSON)
**Silver Schema** = Cleaned, flattened data

This ensures data quality - if the data doesn't match the schema, Spark will catch the error!

---

## Summary

| Stage | File | What It Does |
|-------|------|--------------|
| 1. Ingest | `fetch_movies.py` | Fetches movies from TMDB API → saves to Bronze |
| 2. Transform | `clean_transform.py` | Cleans and enriches data → saves to Silver |
| 3. Analytics | `kpis.py` | Calculates metrics → saves to Gold |
| 4. Visualize | `visualize.py` | Creates charts → saves as PNG |

**Data Flow:**
```
API → Bronze (raw) → Silver (clean) → Gold (metrics) → PNG (visualizations)
```

Each stage builds on the previous one, creating a complete data pipeline!

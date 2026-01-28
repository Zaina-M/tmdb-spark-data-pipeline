# Project Reflection: Movie Data Pipeline


##  Overview

This document captures my learning experience building an end-to-end data pipeline using PySpark, Docker, and the Medallion Architecture. It includes the challenges I faced, how I solved them, and key concepts I learned along the way.

---

##  Project Goal

Build a production-ready data pipeline that:
1. Fetches movie data from the TMDB API
2. Cleans and transforms the data using PySpark
3. Calculates business KPIs
4. Generates visualizations
5. Runs entirely in Docker containers

---

##  What I Learned About Spark

### Why Spark Instead of Pandas?

| Aspect | Pandas | PySpark |
|--------|--------|---------|
| **Data Size** | Single machine memory | Distributed across cluster |
| **Processing** | Single-threaded | Parallel across workers |
| **Best For** | < 1GB data | GB to PB scale |
| **Lazy Evaluation** | No (immediate) | Yes (waits until action) |

**Key Insight:** In this project, the dataset was small enough for Pandas, but I used Spark to learn enterprise patterns. In real jobs, you'll work with millions of rows where Spark is essential.

### Spark Core Concepts I Mastered

#### 1. SparkSession - The Entry Point
```python
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```
**What I learned:** Every Spark application starts with a SparkSession. It's like opening a connection to the Spark engine.

#### 2. DataFrames - Distributed Tables
```python
df = spark.read.json("data/bronze/movies")
df.show(5)
```
**What I learned:** A DataFrame in Spark looks like a Pandas DataFrame, but the data is actually split across multiple machines (or CPU cores in local mode).

#### 3. Transformations vs Actions

| Transformations (Lazy) | Actions (Trigger Execution) |
|------------------------|----------------------------|
| `.filter()` | `.count()` |
| `.select()` | `.show()` |
| `.withColumn()` | `.collect()` |
| `.join()` | `.write()` |

**What I learned:** Spark doesn't actually process data until you call an "action". This confused me at first, I'd write 10 lines of transformations and nothing happened until I called `.show()`.

#### 4. The `col()` Function
```python
from pyspark.sql.functions import col

# Instead of df["column_name"], Spark prefers:
df.filter(col("budget") > 1000000)
```
**What I learned:** Always import `col` from `pyspark.sql.functions`. It's used everywhere and makes code more readable.

#### 5. Handling Nested JSON
```python
# Flatten nested structures
df.withColumn("genres", 
    concat_ws("|", transform(col("genres"), lambda x: x["name"]))
)
```
**Struggle:** The TMDB API returns deeply nested JSON. I had to learn `transform()`, `explode()`, and `concat_ws()` to flatten arrays into pipe-separated strings.

---

##  Architecture Decisions

### Why Medallion Architecture?

```
Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated)
```

**What I learned:**
- **Bronze:** Keep the raw data exactly as it arrived. Never modify it. This is your "backup" if something goes wrong downstream.
- **Silver:** Clean, validate, and structure the data. This is where most transformation happens.
- **Gold:** Pre-calculate metrics so dashboards/reports are fast.

### Why Partition by Date?

```
data/bronze/movies/
├── ingestion_date=2026-01-27/
├── ingestion_date=2026-01-28/
```

**What I learned:** Partitioning allows Spark to read only the data it needs. If I query "show me yesterday's data", Spark skips all other folders.

### Why Add Timestamps to Files?

```
movies_raw_143025.json  # Run at 2:30:25 PM
movies_raw_180000.json  # Run at 6:00:00 PM
```

**Struggle:** Initially, running the pipeline twice in one day would overwrite the previous run. I learned to add timestamps so every run is preserved (audit trail).

---

##  Struggles & How I Solved Them

### Struggle 1: "ModuleNotFoundError" in Docker

**Problem:** My Python imports worked locally but failed in Docker.
```
ModuleNotFoundError: No module named 'src'
```

**Root Cause:** Docker containers have a different file structure. The `PYTHONPATH` wasn't set correctly.

**Solution:** Added environment variable in `docker-compose.yml`:
```yaml
environment:
  - PYTHONPATH=/opt/app
```

**Lesson:** Always check paths when moving from local to container.

---

### Struggle 2: Hardcoded Values Everywhere

**Problem:** Movie IDs, file paths, and thresholds were scattered across multiple files. Changing anything required editing 5 different places.

**Solution:** Created `config/config.yaml` and a config loader utility:
```python
from src.utils.config import get_config
config = get_config()
movie_ids = config.get_movie_ids()
```

**Lesson:** Centralize configuration from the start. It saves hours of debugging later.

---

### Struggle 3: `is_valid_movie()` Returning `None`

**Problem:** My validation function returned `None` instead of `False`:
```python
# Bad: Returns None when title is missing
return isinstance(payload, dict) and payload.get("title") and ...
```

**Root Cause:** Python's `and` operator returns the first falsy value, not a boolean.

**Solution:** Use explicit `if` statements:
```python
if not payload.get("title"):
    return False
return True
```

**Lesson:** Unit tests caught this bug! Always write tests.

---

### Struggle 4: Viewing Data in Jupyter Notebook

**Problem:** The `main()` function in `clean_transform.py` didn't return anything. I couldn't see the results in my notebook.

**Solution:** Modified `main()` to return the DataFrame:
```python
def main():
    # ... transformation logic ...
    return df  # Add this!
```

**Lesson:** Design functions to be usable both as scripts AND as importable modules.

---

### Struggle 5: Spark Session Already Exists

**Problem:** Running cells multiple times in Jupyter gave errors about Spark sessions.

**Root Cause:** `SparkSession.builder.getOrCreate()` was being called multiple times.

**Solution:** Use `.getOrCreate()` instead of `.create()`. It reuses existing sessions.

**Lesson:** Spark sessions are singleton—only one per application.

---

### Struggle 6: Docker Compose Dependencies

**Problem:** The `transform` service started before `ingest` finished, causing missing data errors.

**Solution:** Added `depends_on` in `docker-compose.yml`:
```yaml
transform:
  depends_on:
    - ingest
```

**Lesson:** `depends_on` ensures services start in order, but doesn't wait for completion. For production, use health checks or orchestrators like Airflow.

---

##  Key Takeaways

### 1. Spark is Lazy (and That's Good)
Spark builds an execution plan but doesn't run it until necessary. This allows it to optimize the entire pipeline.

### 2. Schema Enforcement Matters
```python
df = spark.createDataFrame(data, schema=MovieSchema.BRONZE_SCHEMA)
```
Defining schemas upfront catches data quality issues early.

### 3. Testing is Not Optional
Unit tests with fake data caught bugs that would have been nightmare to debug in production.

### 4. Docker = Reproducibility
"It works on my machine" is not acceptable. Docker ensures the pipeline runs the same everywhere.

### 5. Configuration > Hardcoding
Never hardcode values that might change. Use config files or environment variables.

---


##  What I Would Do Differently

1. **Start with tests earlier** - I added tests at the end, but TDD would have saved debugging time.

2. **Use Airflow from the start** - Docker Compose works, but Airflow would give better monitoring and retry logic.

3. **Add data quality checks** - Libraries like Great Expectations would validate data between layers.

4. **Implement incremental loading** - Currently, the pipeline reprocesses all data. Delta Lake would enable efficient updates.

---

##  Resources That Helped Me

1. **TMDB API Docs** - Understanding the data source
2. **Docker Compose Docs** - Multi-container orchestration

---

##  Final Thoughts

This project transformed my understanding of data engineering. Before, I thought it was just "moving data around." Now I understand it's about:

- **Reliability** - Data arrives correctly, every time
- **Scalability** - Can handle 10x growth without rewriting
- **Maintainability** - Someone else can understand and modify it
- **Observability** - You can see what happened and when

The Medallion Architecture isn't just a fancy name, it's a mental model for thinking about data quality and transformation stages.

Spark isn't just "big Pandas", it's a completely different paradigm where you describe WHAT you want, not HOW to compute it.

Docker isn't just for deployment, it's for ensuring your code runs the same way everywhere, eliminating "works on my machine" problems.





# Movie Data Pipeline

A production ready, end-to-end data pipeline that fetches movie data from the TMDB API, processes it through a multi-hop Medallion Architecture (Bronze → Silver → Gold), calculates key business metrics, generates visualizations, and includes comprehensive testing.

##  Architecture

The pipeline follows the **Medallion Architecture** pattern with full containerization:

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              MOVIE DATA PIPELINE                                     │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   ┌──────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐       │
│   │  TMDB    │     │    BRONZE    │     │    SILVER    │     │     GOLD     │       │
│   │   API    │────▶│  Raw JSON/   │────▶│   Cleaned    │────▶│     KPIs     │       │
│   │          │     │   Parquet    │     │   Curated    │     │   Metrics    │       │
│   └──────────┘     └──────────────┘     └──────────────┘     └──────────────┘       │
│        │                  │                    │                    │               │
│        │                  │                    │                    │               │
│        ▼                  ▼                    ▼                    ▼               │
│   ┌──────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐       │
│   │  Config  │     │  Audit Cols  │     │   Schema     │     │Visualizations│       │
│   │  (YAML)  │     │  (timestamp) │     │  Validation  │     │    (.png)    │       │
│   └──────────┘     └──────────────┘     └──────────────┘     └──────────────┘       │
│                                                                                      │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                              DOCKER SERVICES                                         │
│   ┌─────────┐ ┌───────────┐ ┌─────┐ ┌─────────────┐ ┌──────────┐ ┌───────┐          │
│   │ ingest  │ │ transform │ │ kpi │ │visualization│ │ notebook │ │ tests │          │
│   └─────────┘ └───────────┘ └─────┘ └─────────────┘ └──────────┘ └───────┘          │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

| Layer | Purpose | Format | Key Features |
|-------|---------|--------|--------------|
| **Bronze** | Raw ingestion | JSON + Parquet | Partitioned by date, timestamped runs, audit columns |
| **Silver** | Cleaned & curated | Parquet | Flattened JSON, type casting, profit/ROI calculation |
| **Gold** | Business metrics | Parquet | Pre-aggregated KPIs, search queries |
| **Viz** | Reporting | PNG images | Charts and graphs for stakeholders |

##  Project Structure

```text
movie-data-pipeline/
├── config/
│   └── config.yaml              # Centralized configuration (movie IDs, paths, thresholds)
├── ingestion/
│   └── fetch_movies.py          # Step 1: API → Bronze (with retry logic & concurrency)
├── transform/
│   └── clean_transform.py       # Step 2: Bronze → Silver (cleaning & enrichment)
├── analytics/
│   └── kpis.py                  # Step 3: Silver → Gold (KPI calculations)
├── visualization/
│   └── visualize.py             # Step 4: Gold → PNG charts
├── src/
│   ├── schemas.py               # PySpark schema definitions
│   └── utils/
│       ├── config.py            # Configuration loader utility
│       └── logger.py            # Centralized logging
├── tests/
│   ├── test_ingestion.py        # Unit tests for API fetching & validation
│   ├── test_transform.py        # Unit tests for data transformation
│   └── test_kpis.py             # Unit tests for KPI calculations
├── notebooks/
│   └── pipeline_verification.ipynb  # Interactive data inspection
├── docker/
│   ├── Dockerfile               # Spark + Jupyter image
│   └── docker-compose.yml       # Service orchestration
├── data/                        # Data storage (created automatically)
│   ├── bronze/                  # Raw data (partitioned by ingestion_date)
│   ├── silver/                  # Curated data
│   ├── gold/                    # KPI outputs
│   └── visualizations/          # Generated charts
├── .env                         # API credentials (not in git)
├── requirements.txt             # Python dependencies
└── README.md                    # This documentation
```

##  Getting Started

### Prerequisites

- **Docker & Docker Compose** (recommended)
- **TMDB API Key**: Get one [here](https://www.themoviedb.org/documentation/api)

### 1. Environment Setup

```bash
# Clone the repository
git clone <repo-url>
cd movie-data-pipeline

# Create .env file with your API key
echo "TMDB_API_KEY=your_actual_api_key_here" > .env
```

### 2. Configure Movie IDs (Optional)

Edit `config/config.yaml` to customize which movies to fetch:

```yaml
ingestion:
  movie_ids:
    - 299534   
    - 19995    
    - 550      
    # Add more IDs here...
```

##  Running with Docker (Recommended)

### Run the Full Pipeline

```bash
cd docker
docker-compose up --build
```

This runs all stages in sequence: `ingest` → `transform` → `kpi` → `visualization`

### Run Individual Stages

```bash
# Ingestion only
docker-compose run ingest

# Transformation only
docker-compose run transform

# KPIs only
docker-compose run kpi

# Visualizations only
docker-compose run visualization

# Run tests
docker-compose run tests
```

### Interactive Notebook

```bash
docker-compose up notebook
```

Then open `http://localhost:8888` in your browser.

##  Testing

The project includes comprehensive unit tests using pytest:

```bash
# Run all tests
docker-compose run tests

# Run specific test file
docker-compose run tests pytest /opt/app/tests/test_ingestion.py -v

# Run with coverage
docker-compose run tests pytest /opt/app/tests --cov=src --cov=ingestion
```

### Test Coverage

| Module | Tests |
|--------|-------|
| `ingestion` | Movie validation, API retry logic, config loading |
| `transform` | Status filtering, genre flattening, ROI calculation |
| `kpis` | Profit calculation, ranking, franchise analysis |

##  Data Layers Detail

### Bronze Layer (Raw)
- **Path**: `data/bronze/movies/ingestion_date=YYYY-MM-DD/`
- **Format**: JSON + Parquet
- **Features**:
  - Partitioned by ingestion date
  - Timestamped filenames for multiple daily runs (e.g., `movies_raw_143025.json`)
  - Audit columns: `ingestion_timestamp`, `run_id`

### Silver Layer (Curated)
- **Path**: `data/silver/movies_curated/`
- **Format**: Parquet
- **Transformations**:
  - Budget/Revenue converted to millions (USD)
  - Genres flattened to pipe-separated strings
  - Cast/Director extracted from credits
  - ROI and Profit calculated

### Gold Layer (Analytics)
- **Path**: `data/gold/<kpi_name>/`
- **Available KPIs**:
  - `highest_revenue`, `highest_budget`, `highest_profit`
  - `highest_roi`, `lowest_roi` (with budget threshold)
  - `highest_rated`, `lowest_rated`, `most_popular`
  - `top_directors`, `top_franchises`
  - `franchise_vs_standalone`

##  Visualizations

Generated charts in `data/visualizations/`:

| Chart | Description |
|-------|-------------|
| `revenue_vs_budget.png` | Scatter plot of movie financials |
| `roi_by_genre_bar.png` | Average ROI by genre |
| `popularity_vs_rating.png` | Correlation analysis |
| `yearly_trends.png` | Revenue trends over time |
| `franchise_vs_standalone_*.png` | Comparison charts |

##  Configuration

All pipeline settings are centralized in `config/config.yaml`:

```yaml
# API settings
api:
  max_retries: 3
  timeout_seconds: 10

# Which movies to fetch
ingestion:
  movie_ids: [299534, 19995, 140607, ...]

# Data paths
paths:
  bronze: "data/bronze/movies"
  silver: "data/silver/movies_curated"
  gold: "data/gold"

# Thresholds
transformation:
  min_budget_for_roi: 10  # millions USD
  min_votes_for_rating: 10

kpis:
  top_n: 5
```

##  Local Development (Without Docker)

```bash
# Install dependencies
pip install -r requirements.txt

# Run pipeline steps
python ingestion/fetch_movies.py
python transform/clean_transform.py
python analytics/kpis.py
python visualization/visualize.py

# Run tests locally
pytest tests/ -v
```


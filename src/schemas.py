
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, FloatType, 
    DoubleType, DateType, ArrayType, LongType, BooleanType
)


class MovieSchema:
  
    
    # BRONZE LAYER 
    # Raw API Response from TMDB
    
    BRONZE_SCHEMA = StructType([
        StructField("id", IntegerType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("overview", StringType(), nullable=True),
        StructField("tagline", StringType(), nullable=True),
        StructField("release_date", StringType(), nullable=True),  # Will convert to DateType in Silver
        StructField("budget", LongType(), nullable=True),
        StructField("revenue", LongType(), nullable=True),
        StructField("runtime", IntegerType(), nullable=True),
        StructField("vote_average", DoubleType(), nullable=True),
        StructField("vote_count", LongType(), nullable=True),
        StructField("popularity", DoubleType(), nullable=True),
        StructField("original_language", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        
        # JSON nested structures (as-is from API)
        StructField("genres", ArrayType(StructType([
            StructField("id", IntegerType(), nullable=True),
            StructField("name", StringType(), nullable=True)
        ])), nullable=True),
        
        StructField("belongs_to_collection", StructType([
            StructField("id", IntegerType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("poster_path", StringType(), nullable=True),
            StructField("backdrop_path", StringType(), nullable=True)
        ]), nullable=True),
        
        StructField("production_companies", ArrayType(StructType([
            StructField("id", IntegerType(), nullable=True),
            StructField("logo_path", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True),
            StructField("origin_country", StringType(), nullable=True)
        ])), nullable=True),
        
        StructField("production_countries", ArrayType(StructType([
            StructField("iso_3166_1", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True)
        ])), nullable=True),
        
        StructField("spoken_languages", ArrayType(StructType([
            StructField("iso_639_1", StringType(), nullable=True),
            StructField("name", StringType(), nullable=True)
        ])), nullable=True),
        
        StructField("poster_path", StringType(), nullable=True),
        StructField("backdrop_path", StringType(), nullable=True),
        
        # Credits (from append_to_response)
        StructField("credits", StructType([
            StructField("cast", ArrayType(StructType([
                StructField("id", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("character", StringType(), nullable=True),
                StructField("order", IntegerType(), nullable=True)
            ])), nullable=True),
            StructField("crew", ArrayType(StructType([
                StructField("id", IntegerType(), nullable=True),
                StructField("name", StringType(), nullable=True),
                StructField("job", StringType(), nullable=True),
                StructField("department", StringType(), nullable=True)
            ])), nullable=True)
        ]), nullable=True)
    ])
    
    
    # SILVER LAYER 
    # Cleaned, flattened, type-cast data
    
    SILVER_SCHEMA = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("tagline", StringType(), nullable=True),
        StructField("release_date", DateType(), nullable=False),
        StructField("genres", StringType(), nullable=True),  # Pipe-separated: "Action|Adventure|Sci-Fi"
        StructField("belongs_to_collection", StringType(), nullable=True),  # Collection name or null
        StructField("original_language", StringType(), nullable=True),
        StructField("budget_musd", DoubleType(), nullable=True),  # Million USD, 0→NaN, null preserved
        StructField("revenue_musd", DoubleType(), nullable=True),  # Million USD, 0→NaN, null preserved
        StructField("profit_musd", DoubleType(), nullable=True),  # Derived: revenue - budget
        StructField("roi", DoubleType(), nullable=True),  # Derived: revenue / budget (only if budget >= 10M)
        StructField("production_companies", StringType(), nullable=True),  # Pipe-separated
        StructField("production_countries", StringType(), nullable=True),  # Pipe-separated
        StructField("spoken_languages", StringType(), nullable=True),  # Pipe-separated
        StructField("vote_count", LongType(), nullable=True),
        StructField("vote_average", DoubleType(), nullable=True),  # 0-10 scale
        StructField("popularity", DoubleType(), nullable=True),
        StructField("runtime", IntegerType(), nullable=True),  # Minutes
        StructField("overview", StringType(), nullable=True),
        StructField("poster_path", StringType(), nullable=True),
        StructField("backdrop_path", StringType(), nullable=True),
        StructField("cast", StringType(), nullable=True),  # Pipe-separated top cast names
        StructField("cast_size", IntegerType(), nullable=True),  # Number of cast members
        StructField("director", StringType(), nullable=True),  # Primary director name
        StructField("crew_size", IntegerType(), nullable=True),  # Total crew count
        StructField("ingestion_date", DateType(), nullable=False)  # Date ingested from API
    ])
    
    
    # GOLD LAYER 
    # Aggregated KPI results for analytics
    
    KPI_SCHEMA = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("title", StringType(), nullable=False),
        StructField("release_date", DateType(), nullable=True),
        StructField("genres", StringType(), nullable=True),
        StructField("budget_musd", DoubleType(), nullable=True),
        StructField("revenue_musd", DoubleType(), nullable=True),
        StructField("profit_musd", DoubleType(), nullable=True),
        StructField("roi", DoubleType(), nullable=True),
        StructField("vote_average", DoubleType(), nullable=True),
        StructField("vote_count", LongType(), nullable=True),
        StructField("popularity", DoubleType(), nullable=True),
        StructField("runtime", IntegerType(), nullable=True),
        StructField("belongs_to_collection", StringType(), nullable=True)
    ])
    
    
    # FRANCHISE ANALYSIS 
    
    FRANCHISE_COMPARISON_SCHEMA = StructType([
        StructField("is_franchise", StringType(), nullable=False),  # "Franchise" or "Standalone"
        StructField("movie_count", LongType(), nullable=False),
        StructField("mean_revenue", DoubleType(), nullable=True),
        StructField("median_revenue", DoubleType(), nullable=True),
        StructField("mean_budget", DoubleType(), nullable=True),
        StructField("mean_roi", DoubleType(), nullable=True),
        StructField("mean_rating", DoubleType(), nullable=True),
        StructField("mean_popularity", DoubleType(), nullable=True),
        StructField("total_revenue", DoubleType(), nullable=True)
    ])
    
    
#    TOP FRANCHISES 

    TOP_FRANCHISES_SCHEMA = StructType([
        StructField("collection_name", StringType(), nullable=False),
        StructField("movie_count", LongType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=True),
        StructField("mean_revenue", DoubleType(), nullable=True),
        StructField("total_budget", DoubleType(), nullable=True),
        StructField("mean_budget", DoubleType(), nullable=True),
        StructField("mean_rating", DoubleType(), nullable=True),
        StructField("mean_roi", DoubleType(), nullable=True)
    ])
    
    
    #    TOP DIRECTORS 
    
    TOP_DIRECTORS_SCHEMA = StructType([
        StructField("director", StringType(), nullable=False),
        StructField("movie_count", LongType(), nullable=False),
        StructField("total_revenue", DoubleType(), nullable=True),
        StructField("mean_revenue", DoubleType(), nullable=True),
        StructField("mean_rating", DoubleType(), nullable=True),
        StructField("total_movies_revenue", DoubleType(), nullable=True)
    ])
    
    
    @staticmethod
    def validate_schema(df, expected_schema: StructType) -> bool:
        
        # Validate if DataFrame matches expected schema.
        
        
        return df.schema == expected_schema
    
    
    @staticmethod
    def print_schema_comparison(df, expected_schema: StructType) -> None:
        
        # Print comparison between actual and expected schemas for debugging.
       
        actual_fields = {field.name: field.dataType for field in df.schema.fields}
        expected_fields = {field.name: field.dataType for field in expected_schema.fields}
        
        print("\n Schema Comparison:")
        print("\n Fields in both schemas:")
        for field in expected_fields:
            if field in actual_fields and actual_fields[field] == expected_fields[field]:
                print(f"   {field}: {expected_fields[field]}")
        
        print("\n Type mismatches:")
        for field in expected_fields:
            if field in actual_fields and actual_fields[field] != expected_fields[field]:
                print(f"   {field}: {actual_fields[field]} (expected {expected_fields[field]})")
        
        print("\n Missing fields:")
        for field in expected_fields:
            if field not in actual_fields:
                print(f"   {field}: {expected_fields[field]}")
        
        print("\n Extra fields:")
        for field in actual_fields:
            if field not in expected_fields:
                print(f"   {field}: {actual_fields[field]}")

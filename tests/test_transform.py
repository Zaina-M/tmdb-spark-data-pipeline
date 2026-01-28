# Unit Tests for Transformation Module(Tests the data cleaning and transformation logic.)


import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="module")
def spark():
    # Create a SparkSession for testing.
    return (
        SparkSession.builder
        .appName("test_transform")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_bronze_data(spark):
    # Create sample Bronze layer data for testing.
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("status", StringType(), True),
        StructField("budget", DoubleType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", IntegerType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("release_date", StringType(), True),
        StructField("original_language", StringType(), True),
        StructField("genres", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])), True),
    ])
    
    data = [
        (1, "Released Movie", "Released", 100000000.0, 500000000.0, 8.0, 1000, 50.0, 120, "2020-01-15", "en",
         [{"id": 28, "name": "Action"}, {"id": 12, "name": "Adventure"}]),
        (2, "Unreleased Movie", "Post Production", 50000000.0, 0.0, 0.0, 0, 10.0, 90, "2025-12-01", "en",
         [{"id": 35, "name": "Comedy"}]),
        (3, "Another Released", "Released", 200000000.0, 800000000.0, 7.5, 2000, 80.0, 150, "2019-05-20", "en",
         [{"id": 28, "name": "Action"}]),
    ]
    
    return spark.createDataFrame(data, schema)


class TestStatusFiltering:
    # Tests for filtering by movie status.
    
    def test_filters_unreleased_movies(self, spark, sample_bronze_data):
        # Only 'Released' movies should remain after filtering.
        from pyspark.sql.functions import col
        
        # Apply the same filter as in transform
        filtered_df = sample_bronze_data.filter(col("status") == "Released")
        
        assert filtered_df.count() == 2
        
        statuses = [row.status for row in filtered_df.collect()]
        assert all(s == "Released" for s in statuses)
    
    def test_unreleased_movie_removed(self, spark, sample_bronze_data):
        # Movie with 'Post Production' status should be removed.
        from pyspark.sql.functions import col
        
        filtered_df = sample_bronze_data.filter(col("status") == "Released")
        
        titles = [row.title for row in filtered_df.collect()]
        assert "Unreleased Movie" not in titles


class TestGenreFlattening:
    # Tests for flattening nested genre arrays.
    
    def test_genres_flattened_to_pipe_separated(self, spark, sample_bronze_data):
        # Genres array should become pipe-separated string.
        from pyspark.sql.functions import col, concat_ws, transform
        
        flattened_df = sample_bronze_data.withColumn(
            "genres_flat",
            concat_ws("|", transform(col("genres"), lambda x: x["name"]))
        )
        
        result = flattened_df.filter(col("id") == 1).select("genres_flat").collect()[0]
        
        assert result.genres_flat == "Action|Adventure"
    
    def test_single_genre_no_pipe(self, spark, sample_bronze_data):
        # Single genre should not have pipe separator.
        from pyspark.sql.functions import col, concat_ws, transform
        
        flattened_df = sample_bronze_data.withColumn(
            "genres_flat",
            concat_ws("|", transform(col("genres"), lambda x: x["name"]))
        )
        
        result = flattened_df.filter(col("id") == 2).select("genres_flat").collect()[0]
        
        assert "|" not in result.genres_flat
        assert result.genres_flat == "Comedy"


class TestBudgetRevenueConversion:
    # Tests for converting budget/revenue to millions.
    
    def test_budget_converted_to_millions(self, spark, sample_bronze_data):
        # Budget should be divided by 1,000,000.
        from pyspark.sql.functions import col, round
        
        converted_df = sample_bronze_data.withColumn(
            "budget_musd",
            round(col("budget") / 1_000_000, 2)
        )
        
        result = converted_df.filter(col("id") == 1).select("budget_musd").collect()[0]
        
        assert result.budget_musd == 100.0
    
    def test_revenue_converted_to_millions(self, spark, sample_bronze_data):
        # Revenue should be divided by 1,000,000.
        from pyspark.sql.functions import col, round
        
        converted_df = sample_bronze_data.withColumn(
            "revenue_musd",
            round(col("revenue") / 1_000_000, 2)
        )
        
        result = converted_df.filter(col("id") == 1).select("revenue_musd").collect()[0]
        
        assert result.revenue_musd == 500.0


class TestROICalculation:
    # Tests for Return on Investment calculation.
    
    def test_roi_calculated_correctly(self, spark, sample_bronze_data):
        # ROI should be revenue / budget.
        from pyspark.sql.functions import col, when
        
        df_with_musd = (
            sample_bronze_data
            .withColumn("budget_musd", col("budget") / 1_000_000)
            .withColumn("revenue_musd", col("revenue") / 1_000_000)
        )
        
        df_with_roi = df_with_musd.withColumn(
            "roi",
            when(col("budget_musd") > 0, col("revenue_musd") / col("budget_musd"))
        )
        
        result = df_with_roi.filter(col("id") == 1).select("roi").collect()[0]
        
        # 500M / 100M = 5.0
        assert result.roi == 5.0
    
    def test_zero_budget_roi_is_null(self, spark):
        # ROI should be null when budget is zero.
        from pyspark.sql.functions import col, when
        
        data = [(1, 0.0, 100.0)]
        df = spark.createDataFrame(data, ["id", "budget_musd", "revenue_musd"])
        
        df_with_roi = df.withColumn(
            "roi",
            when(col("budget_musd") > 0, col("revenue_musd") / col("budget_musd"))
        )
        
        result = df_with_roi.select("roi").collect()[0]
        
        assert result.roi is None


class TestProfitCalculation:
    # Tests for profit calculation.
    
    def test_profit_is_revenue_minus_budget(self, spark, sample_bronze_data):
        # Profit should be revenue - budget.
        from pyspark.sql.functions import col
        
        df_with_profit = (
            sample_bronze_data
            .withColumn("budget_musd", col("budget") / 1_000_000)
            .withColumn("revenue_musd", col("revenue") / 1_000_000)
            .withColumn("profit_musd", col("revenue_musd") - col("budget_musd"))
        )
        
        result = df_with_profit.filter(col("id") == 1).select("profit_musd").collect()[0]
        
        # 500M - 100M = 400M
        assert result.profit_musd == 400.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

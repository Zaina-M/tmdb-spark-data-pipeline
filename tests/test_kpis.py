# Unit Tests for KPI Module(Tests the analytics and KPI calculation logic.)


import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="module")
def spark():
    # Create a SparkSession for testing.
    return (
        SparkSession.builder
        .appName("test_kpis")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_silver_data(spark):
    # Create sample Silver layer data for KPI testing.
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("budget_musd", DoubleType(), True),
        StructField("revenue_musd", DoubleType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", IntegerType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("genres", StringType(), True),
        StructField("director", StringType(), True),
        StructField("belongs_to_collection", StringType(), True),
    ])
    
    data = [
        (1, "Blockbuster", 200.0, 2000.0, 8.5, 5000, 100.0, "Action|Adventure", "Director A", "Marvel"),
        (2, "Flop", 150.0, 50.0, 4.0, 500, 20.0, "Drama", "Director B", None),
        (3, "Indie Hit", 10.0, 200.0, 9.0, 2000, 60.0, "Drama|Romance", "Director C", None),
        (4, "Sequel", 180.0, 800.0, 7.0, 3000, 80.0, "Action", "Director A", "Marvel"),
        (5, "Low Budget", 5.0, 100.0, 7.5, 100, 30.0, "Horror", "Director D", None),
    ]
    
    return spark.createDataFrame(data, schema)


class TestPrepareKPIs:
    # Tests for the prepare_kpis function.
    
    def test_adds_profit_column(self, spark, sample_silver_data):
        # prepare_kpis should add profit_musd column.
        from analytics.kpis import prepare_kpis
        
        result_df = prepare_kpis(sample_silver_data)
        
        assert "profit_musd" in result_df.columns
    
    def test_profit_calculated_correctly(self, spark, sample_silver_data):
        # Profit should be revenue - budget.
        from analytics.kpis import prepare_kpis
        
        result_df = prepare_kpis(sample_silver_data)
        
        blockbuster = result_df.filter(col("id") == 1).collect()[0]
        
        # 2000 - 200 = 1800
        assert blockbuster.profit_musd == 1800.0
    
    def test_negative_profit_for_flop(self, spark, sample_silver_data):
        # Flop movie should have negative profit.
        from analytics.kpis import prepare_kpis
        
        result_df = prepare_kpis(sample_silver_data)
        
        flop = result_df.filter(col("id") == 2).collect()[0]
        
        # 50 - 150 = -100
        assert flop.profit_musd == -100.0


class TestRankMovies:
    # Tests for the rank_movies function.
    
    def test_ranks_by_revenue_desc(self, spark, sample_silver_data):
        # Should return top N movies by revenue descending.
        from analytics.kpis import rank_movies
        
        result_df = rank_movies(sample_silver_data, "revenue_musd", "desc", 3)
        
        results = result_df.collect()
        
        assert len(results) == 3
        # First should be highest revenue
        assert results[0].title == "Blockbuster"
        assert results[0].revenue_musd == 2000.0
    
    def test_ranks_by_rating_asc(self, spark, sample_silver_data):
        # Should return bottom N movies by rating ascending.
        from analytics.kpis import rank_movies
        
        result_df = rank_movies(sample_silver_data, "vote_average", "asc", 2)
        
        results = result_df.collect()
        
        assert len(results) == 2
        # First should be lowest rated
        assert results[0].title == "Flop"
        assert results[0].vote_average == 4.0
    
    def test_respects_top_n_limit(self, spark, sample_silver_data):
        # Should only return top_n results.
        from analytics.kpis import rank_movies
        
        result_df = rank_movies(sample_silver_data, "popularity", "desc", 2)
        
        assert result_df.count() == 2
    
    def test_applies_filter_expression(self, spark, sample_silver_data):
        # Should apply filter before ranking.
        from analytics.kpis import rank_movies
        
        # Only movies with budget >= 10 (excludes "Low Budget" with 5M)
        result_df = rank_movies(
            sample_silver_data, 
            "revenue_musd", 
            "desc", 
            5,
            filter_expr=col("budget_musd") >= 10
        )
        
        titles = [row.title for row in result_df.collect()]
        
        assert "Low Budget" not in titles


class TestHighestROI:
    # Tests for ROI-based KPIs.
    
    def test_highest_roi_excludes_low_budget(self, spark, sample_silver_data):
        # ROI ranking should exclude movies with very low budgets.
        from analytics.kpis import rank_movies, prepare_kpis
        
        df_with_kpis = prepare_kpis(sample_silver_data)
        
        # Min budget threshold of 10M
        result_df = rank_movies(
            df_with_kpis,
            "roi",
            "desc",
            3,
            filter_expr=col("budget_musd") >= 10
        )
        
        titles = [row.title for row in result_df.collect()]
        
        # "Low Budget" has 5M budget, should be excluded
        assert "Low Budget" not in titles
    
    def test_indie_hit_has_highest_roi(self, spark, sample_silver_data):
        # Indie Hit (10M -> 200M) should have highest ROI among valid movies.
        from analytics.kpis import rank_movies, prepare_kpis
        
        df_with_kpis = prepare_kpis(sample_silver_data)
        
        result_df = rank_movies(
            df_with_kpis,
            "roi",
            "desc",
            1,
            filter_expr=col("budget_musd") >= 10
        )
        
        top_roi = result_df.collect()[0]
        
        # Indie Hit: 200 / 10 = 20x ROI
        assert top_roi.title == "Indie Hit"
        assert top_roi.roi == 20.0


class TestFranchiseAnalysis:
    # Tests for franchise vs standalone analysis.
    
    def test_identifies_franchise_movies(self, spark, sample_silver_data):
        # Movies with belongs_to_collection should be flagged as franchise.
        from pyspark.sql.functions import when
        
        df_with_flag = sample_silver_data.withColumn(
            "is_franchise",
            when(col("belongs_to_collection").isNotNull(), "Franchise").otherwise("Standalone")
        )
        
        franchises = df_with_flag.filter(col("is_franchise") == "Franchise").collect()
        
        assert len(franchises) == 2
        assert all(f.belongs_to_collection == "Marvel" for f in franchises)
    
    def test_standalone_movies_identified(self, spark, sample_silver_data):
        # Movies without collection should be standalone.
        from pyspark.sql.functions import when
        
        df_with_flag = sample_silver_data.withColumn(
            "is_franchise",
            when(col("belongs_to_collection").isNotNull(), "Franchise").otherwise("Standalone")
        )
        
        standalones = df_with_flag.filter(col("is_franchise") == "Standalone").collect()
        
        assert len(standalones) == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

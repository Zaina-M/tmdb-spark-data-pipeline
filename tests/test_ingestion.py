# Unit Tests for Ingestion Module(Tests the movie fetching logic, validation, and error handling.)


import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestMovieValidation:
    # Tests for the is_valid_movie function.
    
    def test_valid_movie_returns_true(self):
        """A complete movie payload should be valid."""
        from ingestion.fetch_movies import is_valid_movie
        
        valid_payload = {
            "id": 299534,
            "title": "Avengers: Endgame",
            "credits": {
                "cast": [{"name": "Robert Downey Jr."}],
                "crew": [{"name": "Joe Russo", "job": "Director"}]
            }
        }
        
        assert is_valid_movie(valid_payload) is True
    
    def test_missing_id_returns_false(self):
        # Movie without ID should be invalid.
        from ingestion.fetch_movies import is_valid_movie
        
        payload = {
            "title": "Test Movie",
            "credits": {"cast": [], "crew": []}
        }
        
        assert is_valid_movie(payload) is False
    
    def test_missing_title_returns_false(self):
        # Movie without title should be invalid.
        from ingestion.fetch_movies import is_valid_movie
        
        payload = {
            "id": 123,
            "credits": {"cast": [], "crew": []}
        }
        
        assert is_valid_movie(payload) is False
    
    def test_missing_credits_returns_false(self):
        # Movie without credits object should be invalid.
        from ingestion.fetch_movies import is_valid_movie
        
        payload = {
            "id": 123,
            "title": "Test Movie"
        }
        
        assert is_valid_movie(payload) is False
    
    def test_api_error_response_returns_false(self):
        # TMDB error response should be invalid.
        from ingestion.fetch_movies import is_valid_movie
        
        error_payload = {
            "success": False,
            "status_code": 34,
            "status_message": "The resource you requested could not be found."
        }
        
        assert is_valid_movie(error_payload) is False
    
    def test_non_dict_returns_false(self):
        # Non-dictionary input should be invalid.
        from ingestion.fetch_movies import is_valid_movie
        
        assert is_valid_movie(None) is False
        assert is_valid_movie("string") is False
        assert is_valid_movie([1, 2, 3]) is False


class TestFetchMovieWithRetries:
    # Tests for the API fetch function with retry logic.
    
    @patch('ingestion.fetch_movies.requests.get')
    def test_successful_fetch_returns_data(self, mock_get):
        """Successful API call should return movie data."""
        from ingestion.fetch_movies import fetch_movie_with_retries
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"id": 123, "title": "Test"}
        mock_get.return_value = mock_response
        
        result = fetch_movie_with_retries(123)
        
        assert result is not None
        assert result["id"] == 123
    
    @patch('ingestion.fetch_movies.requests.get')
    def test_404_returns_none(self, mock_get):
        """404 response should return None without retrying."""
        from ingestion.fetch_movies import fetch_movie_with_retries
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        result = fetch_movie_with_retries(99999)
        
        assert result is None
        # Should only call once (no retries for 404)
        assert mock_get.call_count == 1
    
    @patch('ingestion.fetch_movies.requests.get')
    @patch('ingestion.fetch_movies.time.sleep')
    def test_rate_limit_retries(self, mock_sleep, mock_get):
        """429 rate limit should trigger retry with backoff."""
        from ingestion.fetch_movies import fetch_movie_with_retries
        
        # First call: rate limited, second call: success
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {"Retry-After": "1"}
        
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {"id": 123, "title": "Test"}
        
        mock_get.side_effect = [rate_limit_response, success_response]
        
        result = fetch_movie_with_retries(123)
        
        assert result is not None
        assert mock_get.call_count == 2


class TestConfigIntegration:
    # Tests for configuration loading.
    
    def test_config_loads_movie_ids(self):
        # Config should provide movie IDs list.
        from src.utils.config import get_config
        
        config = get_config()
        movie_ids = config.get_movie_ids()
        
        assert isinstance(movie_ids, list)
        assert len(movie_ids) > 0
        assert all(isinstance(id, int) for id in movie_ids)
    
    def test_config_provides_paths(self):
        # Config should provide data layer paths.
        from src.utils.config import get_config
        
        config = get_config()
        
        assert "bronze" in config.paths
        assert "silver" in config.paths
        assert "gold" in config.paths


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

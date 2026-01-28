"""
Configuration Loader Utility

This module provides a centralized way to load and access pipeline configuration.
It reads from config/config.yaml and provides easy access to all settings.
"""

import os
import yaml
from typing import List, Dict, Any
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Default config path (relative to project root)
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "config",
    "config.yaml"
)


class PipelineConfig:
    """Singleton class to hold pipeline configuration."""
    
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self.load_config()
    
    def load_config(self, config_path: str = None) -> None:
        """Load configuration from YAML file."""
        path = config_path or os.getenv("PIPELINE_CONFIG_PATH", CONFIG_PATH)
        
        # Handle Docker paths
        if os.path.exists("/opt/app/config/config.yaml"):
            path = "/opt/app/config/config.yaml"
        
        try:
            with open(path, "r") as f:
                self._config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {path}")
        except FileNotFoundError:
            logger.warning(f"Config file not found at {path}, using defaults")
            self._config = self._get_defaults()
    
    def _get_defaults(self) -> Dict[str, Any]:
        """Return default configuration if file is missing."""
        return {
            "api": {
                "base_url": "https://api.themoviedb.org/3/movie",
                "max_retries": 3,
                "backoff_multiplier": 2,
                "timeout_seconds": 10
            },
            "ingestion": {
                "method": "static",
                "movie_ids": [299534, 19995, 140607],
                "max_workers": 10
            },
            "paths": {
                "bronze": "data/bronze/movies",
                "silver": "data/silver/movies_curated",
                "gold": "data/gold",
                "visualizations": "data/visualizations"
            },
            "transformation": {
                "min_budget_for_roi": 10,
                "min_votes_for_rating": 10
            },
            "kpis": {
                "top_n": 5
            }
        }
    
    @property
    def api(self) -> Dict[str, Any]:
        """API configuration."""
        return self._config.get("api", {})
    
    @property
    def ingestion(self) -> Dict[str, Any]:
        """Ingestion configuration."""
        return self._config.get("ingestion", {})
    
    @property
    def paths(self) -> Dict[str, str]:
        """Data paths configuration."""
        return self._config.get("paths", {})
    
    @property
    def transformation(self) -> Dict[str, Any]:
        """Transformation configuration."""
        return self._config.get("transformation", {})
    
    @property
    def kpis(self) -> Dict[str, Any]:
        """KPI configuration."""
        return self._config.get("kpis", {})
    
    @property
    def quality(self) -> Dict[str, Any]:
        """Quality thresholds configuration."""
        return self._config.get("quality", {})
    
    def get_movie_ids(self) -> List[int]:
        """Get movie IDs based on ingestion method."""
        return self.ingestion.get("movie_ids", [])
    
    def get_path(self, layer: str) -> str:
        """Get path for a specific data layer."""
        return self.paths.get(layer, f"data/{layer}")


# Global config instance for easy access
def get_config() -> PipelineConfig:
    """Get the global configuration instance."""
    return PipelineConfig()

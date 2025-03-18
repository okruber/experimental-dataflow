"""Base pipeline class with common functionality."""
import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Tuple

from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.options.pipeline_options import PipelineOptions


class BasePipeline(ABC):
    """Base class for all data pipelines."""
    
    def __init__(self, pipeline_options: PipelineOptions):
        """Initialize the pipeline with options."""
        self.pipeline_options = pipeline_options

    @staticmethod
    def read_config(config_path: str) -> Dict:
        """Read configuration from either local file or GCS.
        
        Args:
            config_path: Path to the configuration file (local or GCS).
            
        Returns:
            Dict containing the configuration.
            
        Raises:
            Exception: If there's an error reading the configuration.
        """
        try:
            if config_path.startswith('gs://'):
                # Read from GCS
                gcs_client = GcsIO()
                with gcs_client.open(config_path) as f:
                    return json.loads(f.read().decode('utf-8'))
            else:
                # Read from local file
                with open(config_path, 'r') as config_file:
                    return json.load(config_file)
        except Exception as e:
            logging.error(f"Error reading configuration file {config_path}: {e}")
            raise

    @staticmethod
    def generate_timestamp_info() -> Tuple[str, str, str, str]:
        """Generate timestamp components for folder structure.
        
        Returns:
            Tuple containing (year, month, day, timestamp_suffix).
        """
        now = datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')
        timestamp_suffix = now.strftime('%Y%m%d_%H%M%S')
        return year, month, day, timestamp_suffix

    @staticmethod
    def get_partitioned_path(base_path: str, timestamp_info: Tuple[str, str, str, str]) -> str:
        """Create a partitioned path based on timestamp information.
        
        Args:
            base_path: Base path for the files.
            timestamp_info: Tuple of (year, month, day, timestamp_suffix).
            
        Returns:
            String containing the partitioned path.
        """
        year, month, day, _ = timestamp_info
        return os.path.join(
            base_path,
            f"year={year}",
            f"month={month}",
            f"day={day}"
        )

    @abstractmethod
    def run(self) -> int:
        """Main entry point for the pipeline.
        
        Returns:
            0 for success, non-zero for failure.
        """
        pass
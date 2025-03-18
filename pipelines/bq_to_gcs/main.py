"""
Dataflow batch pipeline to extract data from BigQuery tables and store as parquet files in GCS.
"""
import argparse
import logging
import os
from typing import Dict, List, Tuple, Iterator, Optional, NamedTuple

import apache_beam as beam
import pyarrow as pa
from apache_beam.options.pipeline_options import PipelineOptions

from pipelines.common.base_pipeline import BasePipeline


def bq_schema_to_arrow_schema(bq_schema: List[Dict]) -> pa.Schema:
    """Convert BigQuery schema to Apache Arrow schema.
    
    Args:
        bq_schema: List of BigQuery schema field definitions
        
    Returns:
        Apache Arrow schema
        
    Raises:
        ValueError: If schema conversion fails
    """
    type_mapping = {
        'STRING': pa.string(),
        'INTEGER': pa.int64(),
        'FLOAT': pa.float64(),
        'DATE': pa.date32(),
    }
    
    fields = []
    try:
        for field in bq_schema:
            arrow_type = type_mapping.get(field['type'], pa.string())
            is_nullable = field['mode'] == 'NULLABLE'
            fields.append(pa.field(field['name'], arrow_type, nullable=is_nullable))
        
        return pa.schema(fields)
    except Exception as e:
        error_msg = f"Error converting BigQuery schema to Arrow schema: {e}"
        logging.error(error_msg)
        raise ValueError(error_msg) from e


class TableConfig(NamedTuple):
    """Named tuple for table configuration."""
    source_table: str
    destination_path: str
    file_prefix: str
    arrow_schema: pa.Schema
    table_name: str


class BigQueryToGCSOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--config_path',
            required=True,
            help='Path to the configuration file (local path or GCS URI starting with gs://)'
        )


class BigQueryToGCSPipeline(BasePipeline):
    """Pipeline to extract data from BigQuery tables and store as parquet files in GCS."""

    def prepare_table_config(
        self, 
        table_config: Dict,
        timestamp_info: Tuple[str, str, str, str]
    ) -> TableConfig:
        """Prepare table configuration with computed values.
        
        Args:
            table_config: Raw table configuration from config file
            timestamp_info: Tuple of (year, month, day, timestamp_suffix)
            
        Returns:
            Prepared TableConfig object
            
        Raises:
            ValueError: If table configuration is invalid
        """
        try:
            source_table = table_config['source_table']
            destination_path = table_config['destination_path']
            
            # Get partitioned path
            year, month, day, timestamp_suffix = timestamp_info
            partitioned_path = self.get_partitioned_path(destination_path, timestamp_info)
            
            # Extract table name for file prefix
            table_name = source_table.split('.')[-1]
            
            # Create file name prefix with timestamp
            file_prefix = f"{partitioned_path}/{table_name}_{timestamp_suffix}"
            
            # Convert schema
            arrow_schema = bq_schema_to_arrow_schema(table_config.get('schema', []))
            
            return TableConfig(
                source_table=source_table,
                destination_path=destination_path,
                file_prefix=file_prefix,
                arrow_schema=arrow_schema,
                table_name=table_name
            )
        except Exception as e:
            error_msg = f"Error preparing table config for {table_config.get('source_table', 'unknown')}: {e}"
            logging.error(error_msg)
            raise ValueError(error_msg) from e

    def run(self) -> int:
        """Main entry point for the pipeline.
        
        Returns:
            0 for success, non-zero for failure.
        """
        try:
            # Get the configuration file path from options
            options = self.pipeline_options.view_as(BigQueryToGCSOptions)
            
            # Read configuration using base class method
            config = self.read_config(options.config_path)
            
            # Generate timestamp components using base class method
            timestamp_info = self.generate_timestamp_info()
            
            # Start the pipeline
            with beam.Pipeline(options=self.pipeline_options) as pipeline:
                # Process all tables in parallel by creating sub-pipelines for each table
                for table_entry in config['tables']:
                    try:
                        # Prepare the table configuration
                        table_config = self.prepare_table_config(table_entry, timestamp_info)
                        logging.info(f"Processing table: {table_config.table_name}")
                        
                        # Create a reading transform for the table
                        read_data = (
                            pipeline 
                            | f"Read {table_config.table_name}" >> beam.io.ReadFromBigQuery(
                                table=table_config.source_table
                            )
                        )
                        
                        # Create a writing transform for the table
                        _ = (
                            read_data
                            | f"Write {table_config.table_name} to GCS" >> beam.io.WriteToParquet(
                                file_path_prefix=table_config.file_prefix,
                                schema=table_config.arrow_schema,
                                file_name_suffix=".parquet",
                                record_batch_size=50000
                            )
                        )
                        
                        logging.info(f"Set up pipeline for table: {table_config.table_name}")
                    
                    except Exception as e:
                        logging.error(f"Error setting up pipeline for table: {e}")
            
            return 0
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            return 1


def run(argv=None):
    """Pipeline entry point."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Parse pipeline options
    pipeline_options = BigQueryToGCSOptions(pipeline_args)
    
    pipeline = BigQueryToGCSPipeline(pipeline_options)
    return pipeline.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    exit_code = run()
    exit(exit_code)

    #set
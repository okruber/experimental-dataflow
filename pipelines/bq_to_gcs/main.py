"""
Dataflow batch pipeline to extract data from BigQuery tables and store as parquet files in GCS.
"""
import argparse
import logging
import os
from typing import Dict, List, Tuple

import apache_beam as beam
import pyarrow as pa
from apache_beam.options.pipeline_options import PipelineOptions

from pipelines.common.base_pipeline import BasePipeline


def bq_schema_to_arrow_schema(bq_schema: List[Dict]) -> pa.Schema:
    """Convert BigQuery schema to Apache Arrow schema."""
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
        logging.error(f"Error converting BigQuery schema to Arrow schema: {e}")
        raise


class BigQueryToGCSOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--config_path',
            required=True,
            help='Path to the configuration file (local path or GCS URI starting with gs://)'
        )


class ProcessTableFn(beam.PTransform):
    """PTransform to process a single table, reading from BigQuery and writing to GCS as parquet."""
    
    def __init__(self, source_table: str, file_prefix: str, arrow_schema: pa.Schema):
        super().__init__()
        self.source_table = source_table
        self.file_prefix = file_prefix
        self.arrow_schema = arrow_schema
    
    def expand(self, pcoll):
        """Define the transform operations."""
        # Extract table name for more readable step names
        table_name = self.source_table.split('.')[-1]
        
        data = (
            pcoll.pipeline
            | f"Read {table_name}" >> beam.io.ReadFromBigQuery(
                table=self.source_table
            )
        )
        
        return (
            data
            | f"Write {table_name} to GCS" >> beam.io.WriteToParquet(
                file_path_prefix=self.file_prefix,
                schema=self.arrow_schema,
                file_name_suffix=".parquet",
                record_batch_size=50000
            )
        )


class BigQueryToGCSPipeline(BasePipeline):
    """Pipeline to extract data from BigQuery tables and store as parquet files in GCS."""

    def create_process_table_transform(
        self, 
        table_config: Dict,
        timestamp_info: Tuple[str, str, str, str]
    ) -> ProcessTableFn:
        """Create a ProcessTableFn transform for the given table configuration."""
        try:
            source_table = table_config['source_table']
            destination_path = table_config['destination_path']
            
            # Get partitioned path using base class method
            partitioned_path = self.get_partitioned_path(destination_path, timestamp_info)
            
            # Extract table name for file prefix
            table_name = source_table.split('.')[-1]
            
            # Create file name prefix with timestamp
            file_prefix = f"{partitioned_path}/{table_name}_{timestamp_info[3]}"
            
            # Convert schema
            arrow_schema = bq_schema_to_arrow_schema(table_config.get('schema', []))
            
            return ProcessTableFn(source_table, file_prefix, arrow_schema)
        except Exception as e:
            logging.error(
                f"Error creating process table transform for "
                f"{table_config.get('source_table', 'unknown')}: {e}"
            )
            raise

    def run(self) -> int:
        """Main entry point for the pipeline."""
        try:
            # Get the configuration file path from options
            options = self.pipeline_options.view_as(BigQueryToGCSOptions)
            
            # Read configuration using base class method
            config = self.read_config(options.config_path)
            
            # Generate timestamp components using base class method
            timestamp_info = self.generate_timestamp_info()
            
            # Start the pipeline
            with beam.Pipeline(options=self.pipeline_options) as pipeline:
                # Process all tables in parallel
                for table_config in config['tables']:
                    source_table = table_config['source_table']
                    logging.info(f"Setting up processing for table: {source_table}")
                    
                    # Extract table name for unique transform label
                    table_name = source_table.split('.')[-1]
                    
                    # Create a separate branch for each table with unique label
                    transform = self.create_process_table_transform(
                        table_config, 
                        timestamp_info
                    )
                    _ = pipeline | f"Process {table_name}" >> transform
                    
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
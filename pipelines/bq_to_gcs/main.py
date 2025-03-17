"""
Dataflow batch pipeline to extract data from BigQuery tables and store as parquet files in GCS.
"""
import argparse
import json
import logging
import os
from datetime import datetime

import apache_beam as beam
import pyarrow as pa
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam.options.pipeline_options import PipelineOptions


def bq_schema_to_arrow_schema(bq_schema):
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


def read_config(config_path):
    """Read configuration from either local file or GCS."""
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

class ProcessTableFn(beam.PTransform):
    """PTransform to process a single table, reading from BigQuery and writing to GCS as parquet."""
    
    def __init__(self, source_table, file_prefix, arrow_schema):
        super().__init__()
        self.source_table = source_table
        self.file_prefix = file_prefix
        self.arrow_schema = arrow_schema
    
    def expand(self, pcoll):
        """Define the transform operations."""
        data = (
            pcoll.pipeline
            | f"Read {self.source_table}" >> beam.io.ReadFromBigQuery(
                table=self.source_table
            )
        )
        
        return (
            data
            | f"Write {self.source_table} to {self.file_prefix}" >> beam.io.WriteToParquet(
                file_path_prefix=self.file_prefix,
                schema=self.arrow_schema,
                file_name_suffix=".parquet",
                record_batch_size=50000
            )
        )


def create_process_table_transform(table_config, timestamp_info):
    """
    Create a ProcessTableFn transform for the given table configuration.

    """
    try:
        year, month, day, timestamp_suffix = timestamp_info
        
        source_table = table_config['source_table']
        destination_path = table_config['destination_path']
        
        # Extract dataset and table name for folder structure
        dataset_name = source_table.split('.')[-2]
        table_name = source_table.split('.')[-1]
        
        # Create partitioned folder structure
        partitioned_path = os.path.join(
            destination_path,
            f"year={year}",
            f"month={month}",
            f"day={day}"
        )
        
        # Create file name prefix with timestamp
        file_prefix = f"{partitioned_path}/{table_name}_{timestamp_suffix}"
        
        # Convert schema once and pass to the transform
        arrow_schema = bq_schema_to_arrow_schema(table_config.get('schema', []))
        
        return ProcessTableFn(source_table, file_prefix, arrow_schema)
    except Exception as e:
        logging.error(f"Error creating process table transform for {table_config.get('source_table', 'unknown')}: {e}")
        raise


def generate_timestamp_info():
    """Generate timestamp components for folder structure."""
    now = datetime.now()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    day = now.strftime('%d')
    timestamp_suffix = now.strftime('%Y%m%d_%H%M%S')
    return (year, month, day, timestamp_suffix)


def run(argv=None):
    """Main entry point for the pipeline."""
    try:
        parser = argparse.ArgumentParser()
        known_args, pipeline_args = parser.parse_known_args(argv)

        # Parse pipeline options
        pipeline_options = BigQueryToGCSOptions(pipeline_args)
        
        # Get the configuration file path from options
        options = pipeline_options.view_as(BigQueryToGCSOptions)
        
        # Read configuration from local file or GCS
        config = read_config(options.config_path)
        
        # Generate timestamp components for folder structure - done once at the beginning
        timestamp_info = generate_timestamp_info()
        
        # Start the pipeline
        with beam.Pipeline(options=pipeline_options) as pipeline:
            # Process all tables in parallel
            for i, table_config in enumerate(config['tables']):
                source_table = table_config['source_table']
                logging.info(f"Setting up processing for table: {source_table}")
                
                # Create a separate branch for each table
                transform = create_process_table_transform(table_config, timestamp_info)
                _ = pipeline | f"Process Table {i}" >> transform
                
        return 0
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        return 1


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    exit_code = run()
    exit(exit_code)
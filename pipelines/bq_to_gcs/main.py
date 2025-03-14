"""
Dataflow batch pipeline to extract data from BigQuery tables and store as parquet files in GCS.
"""
import argparse
import json
import logging
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyarrow as pa
from apache_beam.io.gcp.gcsio import GcsIO


def bq_schema_to_arrow_schema(bq_schema):
    """Convert BigQuery schema to Apache Arrow schema."""
    type_mapping = {
        'STRING': pa.string(),
        'INTEGER': pa.int64(),
        'FLOAT': pa.float64(),
        'DATE': pa.date32(),
    }
    
    fields = []
    for field in bq_schema:
        arrow_type = type_mapping.get(field['type'], pa.string())
        is_nullable = field['mode'] == 'NULLABLE'
        fields.append(pa.field(field['name'], arrow_type, nullable=is_nullable))
    
    return pa.schema(fields)


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
    if config_path.startswith('gs://'):
        # Read from GCS
        gcs_client = GcsIO()
        with gcs_client.open(config_path) as f:
            return json.loads(f.read().decode('utf-8'))
    else:
        # Read from local file
        with open(config_path, 'r') as config_file:
            return json.load(config_file)


def run(argv=None):
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Parse pipeline options
    pipeline_options = BigQueryToGCSOptions(pipeline_args)
    
    # Get the configuration file path from options
    options = pipeline_options.view_as(BigQueryToGCSOptions)
    
    # Read configuration from local file or GCS
    config = read_config(options.config_path)
    
    # Start the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        for table_config in config['tables']:
            source_table = table_config['source_table']
            destination_path = table_config['destination_path']
            
            logging.info(f"Processing table: {source_table} to {destination_path}")
            
            # Read from BigQuery
            data = (
                pipeline
                | f"Read {source_table}" >> beam.io.ReadFromBigQuery(
                    table=source_table,
                    use_standard_sql=True
                )
            )
            
            # Write to GCS as parquet
            (
                data
                | f"Write {source_table} to {destination_path}" >> beam.io.WriteToParquet(
                    file_path_prefix=f"{destination_path}/",
                    schema=bq_schema_to_arrow_schema(table_config.get('schema', [])),
                    file_name_suffix=".parquet"
                )
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
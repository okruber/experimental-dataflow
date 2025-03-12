import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class BigQueryToGCSOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--config_path',
            required=True,
            help='Path to the configuration file'
        )


def run(argv=None):
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Parse pipeline options
    pipeline_options = BigQueryToGCSOptions(pipeline_args)
    
    # Get the configuration file path from options
    options = pipeline_options.view_as(BigQueryToGCSOptions)
    
    with open(options.config_path, 'r') as config_file:
        config = json.load(config_file)
    
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
                    schema=beam.io.gcp.bigquery_tools.parse_table_schema_from_json(
                        json.dumps(table_config.get('schema', []))
                    ),
                    file_name_suffix=".parquet"
                )
            )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
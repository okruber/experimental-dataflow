import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomPipelineOptions(PipelineOptions):
    """Custom pipeline options for GCS file transfer."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input_path',
            default='gs://totemic-bucket-raw/input.txt',
            help='Input file path in GCS'
        )
        parser.add_value_provider_argument(
            '--output_path',
            default='gs://totemic-bucket-trusted/input.txt',
            help='Output file path in GCS'
        )

def run_pipeline():
    """Executes the GCS file transfer pipeline."""
    pipeline_options = CustomPipelineOptions()
    
    # Force direct running for local testing
    pipeline_options.view_as(SetupOptions).save_main_session = True

    try:
        with beam.Pipeline(options=pipeline_options) as pipeline:
            (pipeline
             | 'Read from source GCS' >> beam.io.ReadFromText(
                 pipeline_options.input_path)
             | 'Write to destination GCS' >> beam.io.WriteToText(
                 pipeline_options.output_path,
                 num_shards=1,
                 shard_name_template=''))
            
        logger.info('Pipeline completed successfully')
        
    except Exception as e:
        logger.error(f'Pipeline failed: {str(e)}')
        raise

if __name__ == "__main__":
    run_pipeline()
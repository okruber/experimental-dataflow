import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class DataflowOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input', type=str, help='Input file path')
        parser.add_value_provider_argument('--output', type=str, help='Output file path')

def run(argv=None):
    pipeline_options = PipelineOptions()
    dataflow_options = pipeline_options.view_as(DataflowOptions)
    
    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read' >> beam.io.ReadFromText(dataflow_options.input)
         | 'Transform' >> beam.Map(lambda x: x.upper())
         | 'Write' >> beam.io.WriteToText(dataflow_options.output))

if __name__ == '__main__':
    run()
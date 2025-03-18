import setuptools

setuptools.setup(
  name='solita-dataflow-pipelines',
  version='0.1',
  install_requires=[
        'apache-beam[gcp]==2.62.0',
        'pyarrow==16.0.0'
    ],
  packages=setuptools.find_packages(),
)
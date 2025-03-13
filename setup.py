from setuptools import setup, find_packages

setup(
    name='dataflow-pipelines',
    version='0.1',
    description='Dataflow Pipelines for Testing',
    author='Olle Kruber',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]==2.62.0',
        'pyarrow==16.0.0',
        'pandas==2.2.0', # ??
    ],
)
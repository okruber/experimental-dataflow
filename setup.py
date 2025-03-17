from setuptools import setup, find_packages

setup(
    name='dataflow-pipelines-test',
    install_requires=[
        'apache-beam[gcp]==2.62.0',
        'pyarrow==16.0.0'
    ],
    packages=find_packages()
)
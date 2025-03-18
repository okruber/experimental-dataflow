FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="/template/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/streaming_beam.py"

COPY . /template

RUN apt-get update \
    # Upgrade pip and install the requirements.
    && pip install --no-cache-dir --upgrade pip \
    # Install dependencies from requirements file in the launch environment.
    && pip install --no-cache-dir -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE \
    # When FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE  option is used,
    # then during Template launch Beam downloads dependencies
    # into a local requirements cache folder and stages the cache to workers.
    # To speed up Flex Template launch, pre-download the requirements cache
    # when creating the Template.
    && pip download --no-cache-dir --dest /tmp/dataflow-requirements-cache -r $FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
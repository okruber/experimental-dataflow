ARG TAG=latest
# Should be version locked to 20250220-rc00
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base:${TAG}

ARG WORKDIR=/opt/dataflow
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Copy all files
COPY . ${WORKDIR}/

# Read pipeline module from environment file (created during build)
RUN if [ -f pipeline.env ]; then \
    export $(cat pipeline.env | xargs); \
    echo "Building pipeline: $PIPELINE_MODULE"; \
    else \
    echo "No pipeline.env found, unable to determine pipeline module"; \
    exit 1; \
    fi && \
    echo "export PIPELINE_MODULE=$PIPELINE_MODULE" >> /etc/profile.d/pipeline.sh

# Set the pipeline module as an environment variable
ENV PIPELINE_MODULE=${PIPELINE_MODULE}

# Configure the launcher to use the correct pipeline file
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/pipelines/${PIPELINE_MODULE}/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py

# Install apache-beam and other dependencies to launch the pipeline
RUN apt-get update \
    && pip install --no-cache-dir --upgrade pip \
    && pip install 'apache-beam[gcp]==2.62.0' \
    && pip install -U -r ${WORKDIR}/requirements.txt

RUN python setup.py install
ENV PIP_NO_DEPS=True
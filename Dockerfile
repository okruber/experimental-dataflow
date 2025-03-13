ARG TAG=20250220-rc00
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base:${TAG}

ARG WORKDIR=/opt/dataflow
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Copy project files
COPY . ${WORKDIR}/

# Use ARG to get the pipeline module name (passed during build)
ARG PIPELINE_MODULE
RUN if [ -z "$PIPELINE_MODULE" ]; then \
    echo "No PIPELINE_MODULE specified, unable to determine pipeline module"; \
    exit 1; \
    else \
    echo "Building pipeline: $PIPELINE_MODULE"; \
    fi

# Configure the launcher to use the correct pipeline file
ENV PIPELINE_MODULE=${PIPELINE_MODULE}
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/pipelines/${PIPELINE_MODULE}/main.py

# Install dependencies using pip 
# This ensures all dependencies are pre-installed in the container
RUN apt-get update \
    && pip install --no-cache-dir --upgrade pip \
    && pip install 'apache-beam[gcp]==2.62.0' \
    && pip install -U -r ${WORKDIR}/requirements.txt \
    && pip install -e .

# Critical: Do NOT set FLEX_TEMPLATE_PYTHON_SETUP_FILE since we're pre-installing all dependencies
# Remove this line if it was present before:
# ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py

# This tells Dataflow that the container already has all dependencies pre-installed
# and no dependency installation at runtime is needed
ENV PIP_NO_DEPS=True
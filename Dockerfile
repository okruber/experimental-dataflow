# Version-lock template bases and treat as real versioning
ARG TAG=20250220-rc00
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base:${TAG}

# Location for the pipeline artifacts
ARG WORKDIR=/opt/dataflow
WORKDIR ${WORKDIR}

# Get the pipeline module name passed during build
ARG PIPELINE_MODULE
RUN if [ -z "$PIPELINE_MODULE" ]; then \
    echo "No PIPELINE_MODULE specified"; \
    exit 1; \
    else \
    echo "Building pipeline image: $PIPELINE_MODULE"; \
    fi


ENV PIPELINE_MODULE=${PIPELINE_MODULE} \
    FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/pipelines/${PIPELINE_MODULE}/main.py

COPY . ${WORKDIR}/

RUN apt-get update && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -U -r ${WORKDIR}/requirements.txt && \
    pip install --no-cache-dir -e .

# Set this if using Beam 2.37.0 or earlier SDK to speed up job submission :(
ENV PIP_NO_DEPS=True
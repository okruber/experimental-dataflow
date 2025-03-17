# Version-lock template bases and treat as real versioning
ARG TAG=20250220-rc00
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base:${TAG}

ARG WORKDIR=/opt/dataflow
WORKDIR ${WORKDIR}

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
    pip install --no-cache-dir 'apache-beam[gcp]==2.62.0' && \
    pip install --no-cache-dir -U -r ${WORKDIR}/requirements.txt && \
    pip install --no-cache-dir -e .

ENV PIP_NO_DEPS=True
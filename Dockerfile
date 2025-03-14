# Version-lock template bases and treat as real versioning
ARG TAG=20250220-rc00
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base:${TAG}

ARG WORKDIR=/opt/dataflow
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

COPY . ${WORKDIR}/

ARG PIPELINE_MODULE
RUN if [ -z "$PIPELINE_MODULE" ]; then \
    echo "No PIPELINE_MODULE specified"; \
    exit 1; \
    else \
    echo "Building pipeline image: $PIPELINE_MODULE"; \
    fi

ENV PIPELINE_MODULE=${PIPELINE_MODULE}
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/pipelines/${PIPELINE_MODULE}/main.py

RUN apt-get update \
    && pip install --no-cache-dir --upgrade pip \
    && pip install 'apache-beam[gcp]==2.62.0' \
    && pip install -U -r ${WORKDIR}/requirements.txt \
    && pip install -e .

ENV PIP_NO_DEPS=True
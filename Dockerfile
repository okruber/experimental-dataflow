ARG TAG=latest
# Should be version locked to 20250220-rc00
FROM gcr.io/dataflow-templates-base/python312-template-launcher-base:${TAG}

ARG WORKDIR=/opt/dataflow
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

ARG TEMPLATE_NAME=pipeline
COPY . ${WORKDIR}/

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/${TEMPLATE_NAME}/main.py
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE=${WORKDIR}/setup.py

# Install apache-beam and other dependencies to launch the pipeline
RUN apt-get update \
    && pip install --no-cache-dir --upgrade pip \
    && pip install 'apache-beam[gcp]==2.62.0' \
    && pip install -U -r ${WORKDIR}/requirements.txt

RUN python setup.py install
ENV PIP_NO_DEPS=True
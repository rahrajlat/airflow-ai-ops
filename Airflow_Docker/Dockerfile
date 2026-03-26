FROM apache/airflow:3.1.1
LABEL tag="airflow_docker:latest"
COPY docker_requirements.txt .
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir -r docker_requirements.txt
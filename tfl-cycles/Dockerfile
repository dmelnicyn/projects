# First-time build can take up to 10 mins.

FROM apache/airflow:2.2.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install -y vim -qqq
# git gcc g++ -qqq

COPY requirements.txt .

# Switch to airflow user for pip installations
USER $AIRFLOW_UID
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary

# Switch back to root for system installations
USER root

# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# Install Google Cloud SDK from Debian package
RUN apt-get install -y apt-transport-https ca-certificates gnupg \
    && echo "deb https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
    && apt-get update -qq && apt-get install -y google-cloud-sdk

ENV GCLOUD_HOME=/home/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

WORKDIR $AIRFLOW_HOME

USER $AIRFLOW_UID

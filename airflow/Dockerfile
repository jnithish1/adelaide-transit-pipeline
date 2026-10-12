# Airflow image with the pipeline's dependencies baked in.
# Check https://hub.docker.com/r/apache/airflow/tags for the latest 3.x tag.
ARG AIRFLOW_IMAGE=apache/airflow:3.0.2-python3.11
FROM ${AIRFLOW_IMAGE}

# Ingestion libraries go into Airflow's own environment (the tasks import them).
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# dbt gets its own virtualenv: its pinned dependencies regularly clash with
# Airflow's, and keeping them apart is the standard fix.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/airflow/dbt-venv \
 && /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
ENV DBT_BIN=/opt/airflow/dbt-venv/bin/dbt

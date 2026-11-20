FROM ghcr.io/astral-sh/uv:0.12.17 AS uv
FROM apache/airflow:3.3.2-python3.12

COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /opt/renewflow
COPY pyproject.toml uv.lock ./
COPY src ./src
COPY docker/airflow_bootstrap.py /opt/airflow/airflow_bootstrap.py

RUN uv export --locked --no-dev --no-emit-project --no-hashes -o /tmp/renewflow-requirements.txt \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/renewflow-requirements.txt \
    && pip check \
    && mkdir -p /opt/airflow/local

ENV PYTHONPATH=/opt/renewflow/src
WORKDIR /opt/airflow

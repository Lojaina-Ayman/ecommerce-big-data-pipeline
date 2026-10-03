ARG SUPERSET_BASE_IMAGE
FROM ${SUPERSET_BASE_IMAGE}

USER root

RUN pip install --prefix=/app/.venv --no-cache-dir psycopg2-binary clickhouse-connect

USER superset
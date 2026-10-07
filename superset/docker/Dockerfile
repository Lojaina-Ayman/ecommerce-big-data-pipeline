ARG SUPERSET_BASE_IMAGE=apache/superset:6.0.0
FROM ${SUPERSET_BASE_IMAGE}

USER root

RUN pip install --prefix=/app/.venv --no-cache-dir psycopg2-binary "clickhouse-connect>=0.13.0"

USER superset
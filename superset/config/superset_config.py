import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{os.environ['SUPERSET_DB_USER']}:{os.environ['SUPERSET_DB_PASSWORD']}"
    f"@postgres:5432/{os.environ['SUPERSET_DB']}"
)

ROW_LIMIT = 50000
SUPERSET_WEBSERVER_TIMEOUT = 120
# Chart-data cache shared by gunicorn workers, persisted in superset_home volume.
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "FileSystemCache",
    "CACHE_DIR": "/app/superset_home/cache/data",
    "CACHE_DEFAULT_TIMEOUT": 600,
}

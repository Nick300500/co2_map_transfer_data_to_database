"""
Shared helpers for the transfer_*.py scripts in this folder. Standalone on
purpose -- does not import anything from the open-energy-data-server/co2map
repo, so it can live and be run independently of it.

All scripts connect to the same OEDS Postgres DB (host/port from .env,
db "opendata") and write into one dedicated schema (default: cosema_inputs),
kept separate from OEDS's own entsoe/entsoe_raw/... schemas.
"""
import logging
import os

from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TARGET_SCHEMA = os.environ.get("TARGET_SCHEMA", "cosema_inputs")


def db_engine():
    host = os.environ["DB_HOST"]
    port = os.environ.get("DB_PORT", "5432")
    dbname = os.environ.get("DB_NAME", "opendata")
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    logger.info(f"Target DB: {host}:{port}/{dbname}, schema '{TARGET_SCHEMA}'")
    url = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=dbname,
    )
    # fail fast instead of hanging forever on a server that accepts TCP but never answers
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": 30,
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
        },
    )


def ensure_schema(engine, schema: str = TARGET_SCHEMA):
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    logger.info(f"Schema '{schema}' ready.")

"""
Migrates the local open-mastr SQLite database into its own Postgres schema
on the OEDS server (plain tables, no hypertable -- MaStR is a register/master
dataset, not a time series). Table-by-table, chunked writes (the source DB is
~13 GB, too large to load into memory at once).

Writes raw MaStR tables as-is -- no cosema-specific cleaning/aggregation
applied (that logic lives in co2map/cosema/capacities/mastr.py and works on
whatever ends up in this schema, as a later, separate task). This script's
only job is getting the register data into Postgres.

Only migrates the 8 `*_extended` tables that co2map's cosema/capacities/
mastr.py::get_pp_MaStr actually reads (verified by grepping that file for
every other MaStR table name -- none of them turned up). The source SQLite DB
has 38 tables total; the other 30 (market actors, grid infrastructure, gas
market, EEG-subsidy detail tables, open-mastr's own update bookkeeping) are
real MaStR data but untouched by cosema, so left out here to avoid re-syncing
~13 GB of data every run for tables nothing will ever query. Set
MASTR_TABLES in .env (comma-separated) to override -- e.g. to add one of the
skipped tables back in, or to `all` to migrate every table again like before.

Resumable + retries on connection errors (2026-08-24, after a ~19min network/
VPN blip mid-transfer left solar_extended's connection in an unrecoverable
state -- see the PendingRollbackError this used to crash with). Each chunk
write is atomic (to_sql either fully commits or not at all), so on restart
this counts what's already in the target table and skips that many source
rows instead of re-doing the whole (multi-hour, for solar_extended) table
from scratch.
"""
import os
import time

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError, InvalidRequestError

from common import TARGET_SCHEMA, db_engine, ensure_schema, logger

CHUNKSIZE = 50_000
MAX_RETRIES = 5
RETRY_DELAY_SECONDS = 30

DEFAULT_TABLES = [
    f"{tech}_extended"
    for tech in (
        "biomass",
        "combustion",
        "gsgk",
        "hydro",
        "nuclear",
        "solar",
        "storage",
        "wind",
    )
]


def sqlite_engine():
    path = os.environ["MASTR_SQLITE_PATH"]
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"MASTR_SQLITE_PATH={path!r} does not exist -- set the real path "
            "to open-mastr's .db file in .env (see .env.example's comment "
            "about the default open-mastr location)."
        )
    return create_engine(f"sqlite:///{path}")


def _existing_row_count(target_engine, table_name: str) -> int:
    target_table = table_name.lower()
    with target_engine.connect() as conn:
        exists = conn.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_name = :table)"
            ),
            {"schema": TARGET_SCHEMA, "table": target_table},
        ).scalar()
        if not exists:
            return 0
        return conn.execute(
            text(f'SELECT count(*) FROM "{TARGET_SCHEMA}"."{target_table}"')
        ).scalar()


def _write_chunk_with_retry(chunk, target_engine, target_table: str, first_write: bool):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            chunk.to_sql(
                target_table,
                target_engine,
                schema=TARGET_SCHEMA,
                if_exists="replace" if first_write else "append",
                index=False,
                method="multi",
            )
            return
        except (DBAPIError, InvalidRequestError) as e:
            # InvalidRequestError catches PendingRollbackError specifically --
            # once a connection's transaction is left invalid after a dropped
            # connection, the pool needs disposing before a retry can get a
            # clean connection at all, not just "try again".
            target_engine.dispose()
            if attempt == MAX_RETRIES:
                raise
            logger.warning(
                f"{target_table}: write failed ({e.__class__.__name__}), "
                f"retrying in {RETRY_DELAY_SECONDS}s (attempt {attempt}/{MAX_RETRIES})"
            )
            time.sleep(RETRY_DELAY_SECONDS)


def migrate_table(source_engine, target_engine, table_name: str):
    target_table = table_name.lower()
    already_written = _existing_row_count(target_engine, table_name)
    if already_written:
        logger.info(
            f"{table_name}: resuming, {already_written} rows already in target -- "
            "skipping that many source rows"
        )

    total_rows = 0
    first_write = already_written == 0
    with source_engine.connect() as conn:
        chunks = pd.read_sql_table(table_name, conn, chunksize=CHUNKSIZE)
        for chunk in chunks:
            total_rows += len(chunk)
            if total_rows <= already_written:
                continue  # this whole chunk was already written in a previous run

            _write_chunk_with_retry(chunk, target_engine, target_table, first_write)
            first_write = False
            logger.info(f"{table_name}: {total_rows} rows written so far")
    logger.info(f"{table_name}: done, {total_rows} rows total")


def main():
    source_engine = sqlite_engine()
    target_engine = db_engine()
    ensure_schema(target_engine)

    available = inspect(source_engine).get_table_names()

    override = os.environ.get("MASTR_TABLES", "").strip()
    if override.lower() == "all":
        table_names = available
    elif override:
        table_names = [t.strip() for t in override.split(",") if t.strip()]
    else:
        table_names = DEFAULT_TABLES

    missing = set(table_names) - set(available)
    if missing:
        raise ValueError(f"Requested tables not found in source SQLite DB: {sorted(missing)}")

    logger.info(f"Migrating {len(table_names)} of {len(available)} source tables: {table_names}")

    for table_name in table_names:
        migrate_table(source_engine, target_engine, table_name)

    logger.info(f"All tables migrated into schema '{TARGET_SCHEMA}'.")


if __name__ == "__main__":
    main()

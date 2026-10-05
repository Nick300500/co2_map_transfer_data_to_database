"""
Refreshes the MaStR *_extended tables directly from the Bundesnetzagentur
(via the open-mastr package), as a replacement for transfer_mastr.py's
local-SQLite migration -- this one needs no local device, so it's meant to
run on the server. transfer_mastr.py stays untouched for the one-off manual
upload (see PREFECT_PLAN.md, decision 4).

Design (see PREFECT_PLAN.md, "MaStR-Flow: Entscheidungen und Umsetzungsplan"
for the full rationale):
- open-mastr DROPs and recreates each table as it writes it, so writing
  straight into TARGET_SCHEMA would leave it empty/partial for hours.
  Instead, open-mastr writes into a throwaway schema (STAGING_SCHEMA) that
  nobody else reads.
- Once done, each table is checked against the row count already in
  TARGET_SCHEMA: below MIN_ROW_RATIO, the whole refresh is discarded and
  TARGET_SCHEMA is left untouched (open-mastr logs per-file errors instead
  of raising, so a dropped file wouldn't otherwise be noticed).
- If the check passes, the old tables move to BACKUP_SCHEMA (replacing
  whatever was there from the previous run -- one generation, not a
  history) and the new ones move from STAGING_SCHEMA into TARGET_SCHEMA, via
  ALTER TABLE ... SET SCHEMA (a catalog change, not a data copy) in a single
  transaction. Readers therefore only ever see the fully-old or fully-new
  state, never something in between.

open-mastr's own schema handling: internally, every write rebuilds a fresh
engine from str(engine.url) (see its
xml_download/utils_write_to_database.py::create_efficient_engine), and
connect_args passed to our own create_engine() call isn't part of that
string -- it gets dropped. The target schema therefore has to be encoded in
the URL's query string (search_path via `options=-csearch_path=...`), not in
connect_args. Confirmed by a live test against staging on 2026-10-03 (see
PREFECT_PLAN.md): with search_path in connect_args, the first table written
landed in `public`; with it in the URL's query, it landed in STAGING_SCHEMA.
"""
from open_mastr import Mastr
from sqlalchemy import URL, create_engine, text
from sqlalchemy.engine import Engine

from common import TARGET_SCHEMA, db_engine, logger

STAGING_SCHEMA = "mastr_staging"
BACKUP_SCHEMA = "mastr_prev"

MASTR_TECHNOLOGIES = (
    "biomass",
    "combustion",
    "gsgk",
    "hydro",
    "nuclear",
    "solar",
    "storage",
    "wind",
)
MASTR_TABLES = [f"{tech}_extended" for tech in MASTR_TECHNOLOGIES]

# below this fraction of the previous row count, a table is treated as a
# failed/incomplete load and the whole refresh is discarded (open-mastr logs
# per-file write errors instead of raising, so this is the only backstop).
# MaStR only ever grows by a small fraction month over month (new
# registrations), so a real drop of more than ~2% already means something
# is wrong -- this isn't meant as a loose sanity check, it's calibrated to
# how the data actually behaves.
MIN_ROW_RATIO = 0.98


def _staging_engine(db_engine_url: URL) -> Engine:
    """A second engine, same target DB as db_engine() but with its search_path
    pointed at STAGING_SCHEMA -- in the URL itself, not connect_args (see the
    module docstring for why that distinction matters here)."""
    url = db_engine_url.set(
        query={**db_engine_url.query, "options": f"-csearch_path={STAGING_SCHEMA}"}
    )
    return create_engine(url, connect_args={"connect_timeout": 30})


def _table_exists(conn, schema: str, table: str) -> bool:
    return conn.execute(
        text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name = :table)"
        ),
        {"schema": schema, "table": table},
    ).scalar()


def _row_counts(engine: Engine, schema: str, tables: list) -> dict:
    counts = {}
    with engine.connect() as conn:
        for table in tables:
            counts[table] = (
                conn.execute(text(f'SELECT count(*) FROM "{schema}"."{table}"')).scalar()
                if _table_exists(conn, schema, table)
                else 0
            )
    return counts


def swap(target_engine=None):
    """The actual cutover: old tables -> BACKUP_SCHEMA, new ones from
    STAGING_SCHEMA -> TARGET_SCHEMA, in one transaction. Split out from
    main() so it can also be run on its own against an already-populated
    STAGING_SCHEMA (see main()'s `swap` parameter)."""
    target_engine = target_engine or db_engine()
    with target_engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{BACKUP_SCHEMA}" CASCADE'))
        conn.execute(text(f'CREATE SCHEMA "{BACKUP_SCHEMA}"'))
        for table in MASTR_TABLES:
            if _table_exists(conn, TARGET_SCHEMA, table):
                conn.execute(
                    text(f'ALTER TABLE "{TARGET_SCHEMA}"."{table}" SET SCHEMA "{BACKUP_SCHEMA}"')
                )
            conn.execute(
                text(f'ALTER TABLE "{STAGING_SCHEMA}"."{table}" SET SCHEMA "{TARGET_SCHEMA}"')
            )
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{STAGING_SCHEMA}" CASCADE'))
    logger.info(
        f"Getauscht: '{TARGET_SCHEMA}' aktualisiert, alter Stand in '{BACKUP_SCHEMA}' gesichert."
    )


def main(do_swap: bool = True):
    """
    do_swap=False stops right after a passed validation, leaving
    STAGING_SCHEMA populated and untouched -- useful to inspect/test the
    swap step on its own (call swap() separately) instead of chaining it
    onto a multi-hour download run.
    """
    target_engine = db_engine()

    with target_engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{STAGING_SCHEMA}" CASCADE'))
        conn.execute(text(f'CREATE SCHEMA "{STAGING_SCHEMA}"'))
    logger.info(f"'{STAGING_SCHEMA}' bereit (leer).")

    before_counts = _row_counts(target_engine, TARGET_SCHEMA, MASTR_TABLES)
    logger.info(f"Bisheriger Stand in '{TARGET_SCHEMA}': {before_counts}")

    try:
        db = Mastr(engine=_staging_engine(target_engine.url))
        db.download(data=list(MASTR_TECHNOLOGIES))
        logger.info("open-mastr-Download abgeschlossen.")
    except Exception:
        with target_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{STAGING_SCHEMA}" CASCADE'))
        logger.error(f"Download fehlgeschlagen, '{STAGING_SCHEMA}' verworfen.")
        raise

    after_counts = _row_counts(target_engine, STAGING_SCHEMA, MASTR_TABLES)
    logger.info(f"Neuer Stand in '{STAGING_SCHEMA}': {after_counts}")

    shrunk = [
        (table, before_counts[table], after_counts[table])
        for table in MASTR_TABLES
        if after_counts[table] < before_counts[table]
    ]
    for table, old, new in shrunk:
        logger.warning(
            f"{table}: Zeilenzahl gesunken ({old} -> {new}), auch wenn das die "
            f"{int(MIN_ROW_RATIO * 100)}%-Schwelle nicht reißt -- wert, im Blick zu behalten."
        )

    failed = [
        (table, before_counts[table], after_counts[table])
        for table in MASTR_TABLES
        if after_counts[table] < before_counts[table] * MIN_ROW_RATIO
    ]
    if failed:
        with target_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{STAGING_SCHEMA}" CASCADE'))
        details = "; ".join(f"{t}: {old} -> {new}" for t, old, new in failed)
        raise RuntimeError(
            f"Refresh verworfen (< {int(MIN_ROW_RATIO * 100)}% der bisherigen Zeilen): {details}"
        )

    if not do_swap:
        logger.info(
            f"Prüfung bestanden, Tausch übersprungen (do_swap=False) -- "
            f"'{STAGING_SCHEMA}' bleibt vollständig befüllt stehen, zum separaten Testen."
        )
        return

    swap(target_engine)


if __name__ == "__main__":
    main()

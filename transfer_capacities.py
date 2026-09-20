"""
Migrates the monthly capacity parquet snapshots (produced by co2map's
cosema/capacities/mastr.py::calculate_total_capacities_for_cosema, written
locally to inputs/capacities/{Jahr_Monat}/*.parquet) into Postgres.

Two genuinely different file shapes live in that folder, so they get two
separate target tables -- an earlier version of this script concatenated
both into one flat table via pandas.to_sql(), which silently downgraded the
per-plant geometry to opaque WKB-as-text and produced a table with mismatched
columns (NaN-filled depending on which file shape a row came from). Fixed
2026-08-21, after cosema/generation/vre.py turned out to need the geometry
back as a real, queryable PostGIS column (cutout.layout_from_capacity_list
needs actual point geometries, not their WKB serialization). Re-run this
script and it replaces both tables outright (if_exists="replace"); the old
combined "capacities" table is dropped explicitly since nothing reads it
anymore under the new table names.

- conv_capacities_{period}.parquet: one row per federal state (index
  "Region"), one column per conventional technology. No geometry. ->
  capacities_conventional (state, period, <technology columns>).
- {technology}_capacities_{state}_{period}.parquet: one row per individual
  plant (x, y, Capacity, geometry -- a real GeoParquet, read via
  geopandas.read_parquet, no GDAL/fiona needed for that unlike shapefiles).
  -> capacities_vre (technology, state, period, x, y, Capacity, geometry as
  a real PostGIS point column).
"""
import os
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
from sqlalchemy import text

from common import TARGET_SCHEMA, db_engine, ensure_schema, logger

CONV_TABLE = "capacities_conventional"
VRE_TABLE = "capacities_vre"
OLD_TABLE = "capacities"  # superseded, dropped below

CONV_PATTERN = re.compile(r"^conv_capacities_(?P<period>\d{4}_\d{2})\.parquet$")
VRE_PATTERN = re.compile(
    r"^(?P<technology>.+)_capacities_(?P<state>[A-Z]{2,3})_(?P<period>\d{4}_\d{2})\.parquet$"
)


def main():
    base_dir = Path(os.environ["CAPACITIES_DIR"])
    engine = db_engine()
    ensure_schema(engine)

    conv_frames = []
    vre_frames = []

    for parquet_path in sorted(base_dir.glob("*/*.parquet")):
        conv_match = CONV_PATTERN.match(parquet_path.name)
        vre_match = VRE_PATTERN.match(parquet_path.name)

        if conv_match:
            df = pd.read_parquet(parquet_path)
            df.index.name = "state"
            df = df.reset_index()
            df["period"] = conv_match["period"]
            conv_frames.append(df)
            logger.info(f"Read {parquet_path} ({len(df)} rows, conventional)")

        elif vre_match:
            gdf = gpd.read_parquet(parquet_path)
            gdf["technology"] = vre_match["technology"]
            gdf["state"] = vre_match["state"]
            gdf["period"] = vre_match["period"]
            vre_frames.append(gdf)
            logger.info(
                f"Read {parquet_path} ({len(gdf)} rows, "
                f"{vre_match['technology']}/{vre_match['state']})"
            )

        else:
            logger.warning(f"Skipping {parquet_path}: doesn't match a known filename pattern")

    if not conv_frames and not vre_frames:
        raise FileNotFoundError(f"No matching */*.parquet files found under {base_dir}")

    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{TARGET_SCHEMA}"."{OLD_TABLE}"'))

    if conv_frames:
        combined_conv = pd.concat(conv_frames, ignore_index=True)
        combined_conv.to_sql(
            CONV_TABLE, engine, schema=TARGET_SCHEMA, if_exists="replace", index=False
        )
        logger.info(f"{TARGET_SCHEMA}.{CONV_TABLE}: {len(combined_conv)} rows written")

    if vre_frames:
        combined_vre = pd.concat(vre_frames, ignore_index=True)
        combined_vre.to_postgis(
            VRE_TABLE, engine, schema=TARGET_SCHEMA, if_exists="replace", index=False
        )
        logger.info(f"{TARGET_SCHEMA}.{VRE_TABLE}: {len(combined_vre)} rows written")


if __name__ == "__main__":
    main()

"""
Migrates the local shapefiles/geojson used by cosema (see co2map's
config.yaml: `Shapefiles.states.path`, `MaStr.postcodes.path`) into PostGIS
tables on the OEDS server -- PostGIS is already enabled there (confirmed via
the `public` schema's geometry_columns/geography_columns/spatial_ref_sys
tables), so geometries land as real, queryable PostGIS geometry columns
instead of raw files.

Known files, per co2map/config.yaml (adjust FILES below if yours differ):
  - states.geojson       (Shapefiles.states, identifier: NUTS_ID)
  - plz/OSM_PLZ.shp       (MaStr.postcodes)
"""
import os
from pathlib import Path

import geopandas as gpd

from common import TARGET_SCHEMA, db_engine, ensure_schema, logger

# relative to SHAPEFILES_DIR -> target table name in TARGET_SCHEMA
FILES = {
    "states.geojson": "shapefile_states",
    "plz/OSM_PLZ.shp": "shapefile_postcodes",
}


def main():
    base_dir = Path(os.environ["SHAPEFILES_DIR"])
    engine = db_engine()
    ensure_schema(engine)

    for relative_path, table_name in FILES.items():
        path = base_dir / relative_path
        if not path.exists():
            logger.warning(f"Skipping {relative_path}: not found under {base_dir}")
            continue

        gdf = gpd.read_file(path)
        gdf.to_postgis(table_name, engine, schema=TARGET_SCHEMA, if_exists="replace", index=False)
        logger.info(f"{relative_path} -> {TARGET_SCHEMA}.{table_name} ({len(gdf)} rows)")


if __name__ == "__main__":
    main()

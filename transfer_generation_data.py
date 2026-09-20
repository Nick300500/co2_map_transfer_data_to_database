"""
Migrates the small, hand-curated technology mapping CSVs
(gen_types_and_emission_factors.csv, MaStr_gen_types.csv, Matching_idBNA_EIC.csv
-- see co2map's config.yaml MaStr.technologies.carrier_mapping and
cosema/input_output/influxdb.py's module-level gen_types_df read) into
Postgres tables. Tiny files (~49 KB total), one-shot full replace.

Matching_idBNA_EIC.csv added 2026-08-28: missed in the first pass, found only
once cosema/generation/regional_split.py's module-level read of it crashed
the deployed container (it's used to map EIC codes to federal states in
per-unit generation processing).
"""
import os
from pathlib import Path

import pandas as pd

from common import TARGET_SCHEMA, db_engine, ensure_schema, logger

FILES = {
    "gen_types_and_emission_factors.csv": "gen_types_and_emission_factors",
    "MaStr_gen_types.csv": "mastr_gen_types",
    "Matching_idBNA_EIC.csv": "matching_id_bna_eic",
}


def main():
    base_dir = Path(os.environ["GENERATION_DATA_DIR"])
    engine = db_engine()
    ensure_schema(engine)

    for filename, table_name in FILES.items():
        path = base_dir / filename
        if not path.exists():
            logger.warning(f"Skipping {filename}: not found under {base_dir}")
            continue

        df = pd.read_csv(path)
        df.to_sql(table_name, engine, schema=TARGET_SCHEMA, if_exists="replace", index=False)
        logger.info(f"{filename} -> {TARGET_SCHEMA}.{table_name} ({len(df)} rows)")


if __name__ == "__main__":
    main()

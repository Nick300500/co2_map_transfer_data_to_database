"""
Migrates the yearly demand-regionalization-factor CSVs
(inputs/demand_reg_data/demand_reg_factors_{year}.csv, see co2map's
cosema/input_output/local_files.py::load_demand_reg_factors) into one
Postgres table, indexed by time.
"""
import os
from pathlib import Path

import pandas as pd

from common import TARGET_SCHEMA, db_engine, ensure_schema, logger

TABLE_NAME = "demand_reg_factors"


def main():
    base_dir = Path(os.environ["DEMAND_REG_DATA_DIR"])
    engine = db_engine()
    ensure_schema(engine)

    frames = []
    for csv_path in sorted(base_dir.glob("demand_reg_factors_*.csv")):
        df = pd.read_csv(csv_path, index_col=0)
        df.index = pd.to_datetime(df.index, utc=True)
        frames.append(df)
        logger.info(f"Read {csv_path} ({len(df)} rows)")

    if not frames:
        raise FileNotFoundError(
            f"No demand_reg_factors_*.csv files found under {base_dir}"
        )

    combined = pd.concat(frames)
    combined = combined[~combined.index.duplicated(keep="first")]
    combined.index.name = "time"
    combined = combined.reset_index()

    combined.to_sql(
        TABLE_NAME, engine, schema=TARGET_SCHEMA, if_exists="replace", index=False
    )
    logger.info(f"{TARGET_SCHEMA}.{TABLE_NAME}: {len(combined)} rows written")


if __name__ == "__main__":
    main()

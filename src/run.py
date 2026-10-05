import os
import shutil

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

from src.pipeline import (
    DATA_DIR,
    download,
    geocode_collisions,
    load_addresses,
    load_collisions,
    load_intersections,
    processed_file,
    split_collisions,
)


def load_to_postgis():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required")
    intersections = gpd.read_parquet(processed_file("final_geocoded_intersection_collisions.parquet"))
    addresses = gpd.read_parquet(processed_file("final_geocoded_address_collisions.parquet"))
    combined = gpd.GeoDataFrame(
        pd.concat([intersections, addresses], ignore_index=True),
        geometry="geometry",
        crs=intersections.crs,
    )
    combined.to_postgis("geocoded_collisions", create_engine(database_url), if_exists="replace", index=False)


def main():
    download()
    load_collisions()
    load_intersections()
    load_addresses()
    split_collisions()
    geocode_collisions()
    load_to_postgis()
    shutil.rmtree(DATA_DIR / "processed", ignore_errors=True)


if __name__ == "__main__":
    main()

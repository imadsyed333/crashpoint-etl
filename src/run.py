import json
import os
import shutil
import urllib.request

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine

from src.pipeline import (
    DATA_DIR,
    geocode_collisions,
    load_addresses,
    load_collisions,
    load_intersections,
    processed_file,
    split_collisions,
)

CKAN = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action"
DUMP_BASE = "https://ckan0.cf.opendata.inter.prod-toronto.ca/datastore/dump"
PACKAGES = {
    "collisions.csv": "motor-vehicle-collisions-involving-killed-or-seriously-injured-persons",
    "addresses.csv": "address-points-municipal-toronto-one-address-repository",
    "intersections.csv": "intersection-file-city-of-toronto",
}


def datastore_dump_url(package_id):
    req = urllib.request.Request(
        f"{CKAN}/package_show?id={package_id}",
        headers={"User-Agent": "crashpoint-etl"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        resources = json.load(resp)["result"]["resources"]
    for res in resources:
        url = res.get("url") or ""
        if "/datastore/dump/" in url:
            return url
    for res in resources:
        if res.get("datastore_active"):
            return f"{DUMP_BASE}/{res['id']}"
    raise RuntimeError(f"No datastore dump for {package_id}")


def download():
    raw = DATA_DIR / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    for filename, package_id in PACKAGES.items():
        dest = raw / filename
        url = datastore_dump_url(package_id)
        print(f"Downloading {package_id} -> {dest}")
        urllib.request.urlretrieve(url, dest)


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

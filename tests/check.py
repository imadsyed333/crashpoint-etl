"""Synthetic split → similarity match → distance check. Fails if geocoding logic breaks."""
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ["DATA_DIR"] = tempfile.mkdtemp()

import pandas as pd  # noqa: E402

from src.pipeline import (  # noqa: E402
    DATA_DIR,
    geocode_collisions,
    load_addresses,
    load_collisions,
    load_intersections,
    processed_file,
    raw_file,
    split_collisions,
)

LON, LAT = -79.38, 43.65
FAR_LON, FAR_LAT = -79.37, 43.65
NEAR_POINT = json.dumps({"type": "Point", "coordinates": [LON, LAT]})
FAR_POINT = json.dumps({"type": "Point", "coordinates": [FAR_LON, FAR_LAT]})


def write_csvs():
    raw = DATA_DIR / "raw"
    raw.mkdir(parents=True)
    pd.DataFrame(
        [
            {"collision_id": 1, "stname1": "123 Main", "stname2": "", "stname3": "", "latitude": LAT, "longitude": LON},
            {"collision_id": 1, "stname1": "123 Main", "stname2": "", "stname3": "", "latitude": LAT, "longitude": LON},
            {"collision_id": 2, "stname1": "Queen", "stname2": "Spadina", "stname3": "", "latitude": LAT, "longitude": LON},
        ]
    ).to_csv(raw_file("collisions.csv"), index=False)
    pd.DataFrame(
        [
            {"ADDRESS_POINT_ID": 99, "ADDRESS_FULL": "1 King", "geometry": NEAR_POINT},
            {"ADDRESS_POINT_ID": 10, "ADDRESS_FULL": "123 Main", "geometry": FAR_POINT},
        ]
    ).to_csv(raw_file("addresses.csv"), index=False)
    pd.DataFrame(
        [
            {"INTERSECTION_ID": 99, "INTERSECTION_DESC": "King / Yonge", "geometry": NEAR_POINT},
            {"INTERSECTION_ID": 20, "INTERSECTION_DESC": "Queen / Spadina", "geometry": FAR_POINT},
        ]
    ).to_csv(raw_file("intersections.csv"), index=False)


def main():
    write_csvs()
    load_collisions()
    load_intersections()
    load_addresses()
    split_collisions()
    geocode_collisions()

    import geopandas as gpd

    collisions = gpd.read_parquet(processed_file("collisions.parquet"))
    assert len(collisions) == 2, f"expected collision_id dedupe to 2 rows, got {len(collisions)}"

    addresses = gpd.read_parquet(processed_file("final_geocoded_address_collisions.parquet"))
    intersections = gpd.read_parquet(processed_file("final_geocoded_intersection_collisions.parquet"))
    assert len(addresses) == 1 and len(intersections) == 1
    assert addresses["feature_id"].iloc[0] == 10
    assert intersections["feature_id"].iloc[0] == 20
    assert addresses["similarity_score"].notna().all()
    assert intersections["similarity_score"].notna().all()
    assert addresses["similarity_score"].iloc[0] >= 80
    assert intersections["similarity_score"].iloc[0] >= 80
    assert addresses["distance"].iloc[0] > 100
    assert intersections["distance"].iloc[0] > 100
    print("ok")


if __name__ == "__main__":
    main()

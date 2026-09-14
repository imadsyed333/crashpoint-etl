"""Synthetic split → nearest join → similarity check. Fails if geocoding logic breaks."""
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
    compute_similarity,
    geocode_collisions,
    load_addresses,
    load_collisions,
    load_intersections,
    processed_file,
    raw_file,
    split_collisions,
)

LON, LAT = -79.38, 43.65
POINT = json.dumps({"type": "Point", "coordinates": [LON, LAT]})


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
        [{"ADDRESS_POINT_ID": 10, "ADDRESS_FULL": "123 Main", "geometry": POINT}]
    ).to_csv(raw_file("addresses.csv"), index=False)
    pd.DataFrame(
        [{"INTERSECTION_ID": 20, "INTERSECTION_DESC": "Queen / Spadina", "geometry": POINT}]
    ).to_csv(raw_file("intersections.csv"), index=False)


def main():
    write_csvs()
    load_collisions()
    load_intersections()
    load_addresses()
    split_collisions()
    geocode_collisions()
    compute_similarity()

    import geopandas as gpd

    collisions = gpd.read_parquet(processed_file("collisions.parquet"))
    assert len(collisions) == 2, f"expected collision_id dedupe to 2 rows, got {len(collisions)}"

    addresses = gpd.read_parquet(processed_file("final_geocoded_address_collisions.parquet"))
    intersections = gpd.read_parquet(processed_file("final_geocoded_intersection_collisions.parquet"))
    assert len(addresses) == 1 and len(intersections) == 1
    assert addresses["similarity_score"].notna().all()
    assert intersections["similarity_score"].notna().all()
    assert addresses["similarity_score"].iloc[0] >= 80
    assert intersections["similarity_score"].iloc[0] >= 80
    print("ok")


if __name__ == "__main__":
    main()

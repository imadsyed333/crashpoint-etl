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

from src.dashboard import (  # noqa: E402
    DISTANCE_EDGES,
    DISTANCE_LABELS,
    SCORE_EDGES,
    SCORE_LABELS,
    apply_filters,
    bucket_counts,
    bucket_labels,
    histogram,
    picked_row,
)
from src.pipeline import (  # noqa: E402
    DATA_DIR,
    address_pattern,
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


def check_address_pattern():
    matched = pd.Series(["1755 LAKE SHORE BLVD W", "123 Main"]).str.match(address_pattern, case=False)
    assert matched.all()
    unmatched = pd.Series(["27 HWY N", "401 Hwy", "Queen"]).str.match(address_pattern, case=False)
    assert not unmatched.any()


def check_filters():
    rows = pd.DataFrame([
        {
            "collision_id": 10, "stname1": "Queen", "stname2": "Spadina", "stname3": "",
            "description": "queen / spadina", "type": "intersection",
            "similarity_score": 90.0, "distance": 20.0,
        },
        {
            "collision_id": 11, "stname1": "123 Main", "stname2": "", "stname3": "",
            "description": "123 main st", "type": "address",
            "similarity_score": 40.0, "distance": 500.0,
        },
    ])
    assert list(apply_filters(rows, "all", 0, 0, "")["collision_id"]) == [10, 11]
    assert list(apply_filters(rows, "all", 0, 0, "10")["collision_id"]) == [10]
    assert list(apply_filters(rows, "all", 0, 0, "spadina")["collision_id"]) == [10]
    assert list(apply_filters(rows, "all", 0, 0, "123 MAIN")["collision_id"]) == [11]
    assert list(apply_filters(rows, "intersection", 0, 0, "")["collision_id"]) == [10]
    assert list(apply_filters(rows, "all", 80, 0, "")["collision_id"]) == [10]
    assert list(apply_filters(rows, "all", 0, 100, "")["collision_id"]) == [11]
    assert apply_filters(rows, "all", 0, 0, "(").empty


def count_at(frame, bucket, kind):
    hit = frame[(frame["bucket"].astype(str) == bucket) & (frame["type"] == kind)]
    assert len(hit) == 1, f"missing {kind} {bucket}"
    return hit.iloc[0]


def check_match_charts():
    rows = pd.DataFrame({
        "similarity_score": [49, 50, 100, 0],
        "distance": [0.0, 100.0, 1000.0, 5000.0],
        "type": ["intersection", "address", "intersection", "address"],
    })
    scores = bucket_counts(rows, "similarity_score", SCORE_EDGES, SCORE_LABELS)
    scores["label"] = bucket_labels(scores)
    assert count_at(scores, "40–50", "intersection")["collisions"] == 1
    assert count_at(scores, "40–50", "intersection")["label"] == "50%"
    assert count_at(scores, "50–60", "address")["collisions"] == 1
    assert count_at(scores, "90–100", "intersection")["collisions"] == 1
    assert count_at(scores, "0–10", "address")["collisions"] == 1
    assert count_at(scores, "0–10", "address")["label"] == "50%"
    assert count_at(scores, "0–10", "intersection")["collisions"] == 0
    assert count_at(scores, "0–10", "intersection")["label"] == ""
    spec = histogram(scores, "Similarity score", SCORE_LABELS).to_dict()
    assert spec["layer"][1]["mark"]["type"] == "text"
    distances = bucket_counts(rows, "distance", DISTANCE_EDGES, DISTANCE_LABELS)
    assert count_at(distances, "0–100", "intersection")["collisions"] == 1
    assert count_at(distances, "100–200", "address")["collisions"] == 1
    assert count_at(distances, "900–1000", "intersection")["collisions"] == 0
    assert count_at(distances, ">1000", "intersection")["collisions"] == 1
    assert count_at(distances, ">1000", "address")["collisions"] == 1


def check_picked_row():
    row = {"stname1": "Queen", "stname2": "Spadina"}
    assert picked_row({"objects": {"collision": [row]}}) is row
    assert picked_row({"objects": {"link": [], "collision": [row]}}) is row
    assert picked_row({"objects": {}}) is None


def main():
    check_address_pattern()
    check_filters()
    check_match_charts()
    check_picked_row()
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
    assert abs(addresses["match_latitude"].iloc[0] - FAR_LAT) < 1e-5
    assert abs(addresses["match_longitude"].iloc[0] - FAR_LON) < 1e-5
    assert abs(intersections["match_latitude"].iloc[0] - FAR_LAT) < 1e-5
    assert abs(intersections["match_longitude"].iloc[0] - FAR_LON) < 1e-5
    print("ok")


if __name__ == "__main__":
    main()

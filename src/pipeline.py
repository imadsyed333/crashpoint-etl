import json
import os
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from rapidfuzz import fuzz, process
from shapely.geometry import shape

address_pattern = r'^\d{1,5}\w?\s{0,2}\w+\s?\w+$'

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data"))


def raw_file(name):
    return DATA_DIR / "raw" / name


def processed_file(name):
    path = DATA_DIR / "processed"
    path.mkdir(parents=True, exist_ok=True)
    return path / name


def to_point(geom):
    if geom.geom_type == 'Point':
        return geom
    elif geom.geom_type == 'MultiPoint':
        return geom.centroid
    else:
        raise ValueError(f"Unsupported geometry type: {geom.geom_type}")


def load_collisions():
    collision_data = pd.read_csv(raw_file("collisions.csv"))

    collision_data = collision_data[["collision_id", "stname1", "stname2", "stname3", "latitude", "longitude"]]
    collision_data = collision_data.drop_duplicates(subset=["collision_id"])

    collision_data = gpd.GeoDataFrame(
        collision_data,
        geometry=gpd.points_from_xy(collision_data.longitude, collision_data.latitude),
        crs=4326,
    ).to_crs(32617)

    collision_data.to_parquet(processed_file("collisions.parquet"), index=False)


def load_intersections():
    intersection_data = pd.read_csv(raw_file("intersections.csv"))

    intersection_data = intersection_data.rename(columns={'INTERSECTION_ID': 'feature_id', 'INTERSECTION_DESC': 'description'})

    intersection_data['type'] = 'intersection'

    intersection_data = intersection_data[['feature_id', 'description', 'type', 'geometry']]

    intersection_data["geometry"] = intersection_data["geometry"].apply(lambda x: shape(json.loads(x)))
    intersection_data = gpd.GeoDataFrame(intersection_data, geometry='geometry', crs=4326).to_crs(32617)

    intersection_data['geometry'] = intersection_data['geometry'].apply(to_point)

    intersection_data.to_parquet(processed_file("intersections.parquet"), index=False)


def load_addresses():
    address_data = pd.read_csv(raw_file("addresses.csv"))

    address_data = address_data.rename(columns={"ADDRESS_POINT_ID": "feature_id", "ADDRESS_FULL": "description"})

    address_data['type'] = 'address'

    address_data = address_data[["feature_id", "description", 'type', "geometry"]]

    address_data["geometry"] = address_data["geometry"].apply(lambda x: shape(json.loads(x)))

    address_data = gpd.GeoDataFrame(address_data, geometry="geometry", crs=4326).to_crs(32617)

    address_data["geometry"] = address_data["geometry"].apply(to_point)

    address_data.to_parquet(processed_file("addresses.parquet"), index=False)


def split_collisions():
    collisions = gpd.read_parquet(processed_file("collisions.parquet"))

    address_collisions = collisions[collisions['stname1'].str.match(address_pattern, na=False) | collisions['stname2'].str.match(address_pattern, na=False)]

    intersection_collisions = collisions[~collisions['collision_id'].isin(address_collisions['collision_id'])]

    intersection_collisions.to_parquet(processed_file("intersection_collisions.parquet"), index=False)
    address_collisions.to_parquet(processed_file("address_collisions.parquet"), index=False)


def match_by_similarity(collisions, features, *, strip_slash=False):
    collisions = collisions.reset_index(drop=True)
    features = features.reset_index(drop=True)

    location_description = (collisions["stname1"].fillna("") + " " + collisions["stname2"].fillna("")).str.lower()
    choices = features["description"].fillna("").str.lower()
    if strip_slash:
        choices = choices.str.replace("/", "", regex=False)

    queries = location_description.to_numpy()
    choices_arr = choices.to_numpy()
    unique_queries, inverse = np.unique(queries, return_inverse=True)

    unique_best_idx = np.empty(len(unique_queries), dtype=np.intp)
    unique_best_score = np.empty(len(unique_queries), dtype=np.float32)
    # ponytail: cap cdist matrix at ~128MB float32; raise if runners have more RAM
    batch = max(1, min(len(unique_queries), 32_000_000 // max(len(choices_arr), 1)))
    choices_list = choices_arr.tolist()
    for start in range(0, len(unique_queries), batch):
        chunk = unique_queries[start:start + batch].tolist()
        scores = process.cdist(chunk, choices_list, scorer=fuzz.token_set_ratio, dtype=np.float32, workers=-1)
        unique_best_idx[start:start + batch] = scores.argmax(axis=1)
        unique_best_score[start:start + batch] = scores.max(axis=1)

    best_idx = unique_best_idx[inverse]
    matched = features.iloc[best_idx].reset_index(drop=True)

    out = collisions.copy()
    out["location_description"] = location_description.to_numpy()
    out["feature_id"] = matched["feature_id"].to_numpy()
    out["description"] = choices_arr[best_idx]
    out["type"] = matched["type"].to_numpy()
    out["similarity_score"] = unique_best_score[inverse]
    out["distance"] = shapely.distance(out.geometry.values, matched.geometry.values)
    return out


def geocode_collisions():
    intersection_collisions = gpd.read_parquet(processed_file("intersection_collisions.parquet"))
    address_collisions = gpd.read_parquet(processed_file("address_collisions.parquet"))

    intersections = gpd.read_parquet(processed_file("intersections.parquet"))
    addresses = gpd.read_parquet(processed_file("addresses.parquet"))

    geocoded_intersection_collisions = match_by_similarity(intersection_collisions, intersections, strip_slash=True)
    geocoded_address_collisions = match_by_similarity(address_collisions, addresses)
    geocoded_address_collisions["stname2"] = geocoded_address_collisions["stname2"].fillna("")

    geocoded_intersection_collisions.to_parquet(processed_file("final_geocoded_intersection_collisions.parquet"), index=False)
    geocoded_address_collisions.to_parquet(processed_file("final_geocoded_address_collisions.parquet"), index=False)

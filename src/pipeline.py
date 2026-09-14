import json
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from rapidfuzz import fuzz
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


def geocode_collisions():
    intersection_collisions = gpd.read_parquet(processed_file("intersection_collisions.parquet"))
    address_collisions = gpd.read_parquet(processed_file("address_collisions.parquet"))

    intersections = gpd.read_parquet(processed_file("intersections.parquet"))
    addresses = gpd.read_parquet(processed_file("addresses.parquet"))

    geocoded_intersection_collisions = gpd.sjoin_nearest(intersection_collisions, intersections, how="left", distance_col="distance")

    geocoded_address_collisions = gpd.sjoin_nearest(address_collisions, addresses, how="left", distance_col="distance")

    geocoded_intersection_collisions.to_parquet(processed_file("geocoded_intersection_collisions.parquet"), index=False)
    geocoded_address_collisions.to_parquet(processed_file("geocoded_address_collisions.parquet"), index=False)


def compute_similarity():
    geocoded_intersection_collisions = gpd.read_parquet(processed_file("geocoded_intersection_collisions.parquet"))
    geocoded_address_collisions = gpd.read_parquet(processed_file("geocoded_address_collisions.parquet"))

    geocoded_intersection_collisions['location_description'] = (geocoded_intersection_collisions['stname1'] + " " + geocoded_intersection_collisions['stname2']).str.lower()

    geocoded_intersection_collisions['description'] = geocoded_intersection_collisions['description'].str.lower().str.replace("/", "")

    geocoded_address_collisions['stname2'] = geocoded_address_collisions['stname2'].fillna("")

    geocoded_address_collisions['location_description'] = (geocoded_address_collisions['stname1'] + " " + geocoded_address_collisions['stname2']).str.lower()

    geocoded_address_collisions['description'] = geocoded_address_collisions['description'].str.lower()

    geocoded_intersection_collisions['similarity_score'] = [fuzz.token_set_ratio(loc_desc, desc) for loc_desc, desc in zip(geocoded_intersection_collisions['location_description'], geocoded_intersection_collisions['description'])]

    geocoded_address_collisions['similarity_score'] = [fuzz.token_set_ratio(loc_desc, desc) for loc_desc, desc in zip(geocoded_address_collisions['location_description'], geocoded_address_collisions['description'])]

    geocoded_intersection_collisions.to_parquet(processed_file("final_geocoded_intersection_collisions.parquet"), index=False)
    geocoded_address_collisions.to_parquet(processed_file("final_geocoded_address_collisions.parquet"), index=False)

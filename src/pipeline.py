import geopandas as gpd
import pandas as pd
from shapely.geometry import shape
import json
from rapidfuzz import process, fuzz

address_pattern = r'^\d{1,5}\w?\s{0,2}\w+\s?\w+$'

CONTAINER_PATH = "/opt/airflow"

def to_point(geom):
    if geom.geom_type == 'Point':
        return shape(geom)
    elif geom.geom_type == 'MultiPoint':
        return shape(geom).centroid
    else:
        raise ValueError(f"Unsupported geometry type: {geom.geom_type}")

def load_collisions():
    # Initialize the collision data
    collision_data = pd.read_csv(f"{CONTAINER_PATH}/data/raw/collisions.csv")

    # Prune to necessary columns
    collision_data = collision_data[["collision_id", "stname1", "stname2", "stname3", "latitude", "longitude"]]

    # Converting to geodataframe to build geometry
    collision_data = gpd.GeoDataFrame(collision_data, geometry=gpd.points_from_xy(collision_data.longitude, collision_data.latitude), crs=32617)

    # Saving to parquet to preserve geometry and data for next step
    collision_data.to_parquet(f"{CONTAINER_PATH}/data/processed/collisions.parquet", index=False)

def load_intersections():
    # Initialize the intersection data
    intersection_data = pd.read_csv(f"{CONTAINER_PATH}/data/raw/intersections.csv")

    # Rename columns for consistency
    intersection_data = intersection_data.rename(columns={'INTERSECTION_ID': 'feature_id', 'INTERSECTION_DESC': 'description'})

    # Adding type column for use with address data
    intersection_data['type'] = 'intersection'

    # Extract only necessary columns
    intersection_data = intersection_data[['feature_id', 'description', 'type', 'geometry']]

    # Convert the geometry from WKT to shapely geometry
    intersection_data["geometry"] = intersection_data["geometry"].apply(lambda x: shape(json.loads(x)))
    intersection_data = gpd.GeoDataFrame(intersection_data, geometry='geometry', crs=32617)

    intersection_data['geometry'] = intersection_data['geometry'].apply(lambda x: to_point(x))

    intersection_data.to_parquet(f"{CONTAINER_PATH}/data/processed/intersections.parquet", index=False)

    intersection_data.head()

def load_addresses():
    # Initialize the address data
    address_data = pd.read_csv(f"{CONTAINER_PATH}/data/raw/addresses.csv")

    # Renaming columns for consistency
    address_data = address_data.rename(columns={"ADDRESS_POINT_ID": "feature_id", "ADDRESS_FULL": "description"})

    # Adding type column for use with intersection data
    address_data['type'] = 'address'

    # Extracting necessary columns
    address_data = address_data[["feature_id", "description", 'type', "geometry"]]

    # Constructing geometry 
    address_data["geometry"] = address_data["geometry"].apply(lambda x: shape(json.loads(x)))

    address_data = gpd.GeoDataFrame(address_data, geometry="geometry", crs=32617)

    # Cleaning up multipoint geometry
    address_data["geometry"] = address_data["geometry"].apply(lambda x: to_point(x))

    address_data.to_parquet(f"{CONTAINER_PATH}/data/processed/addresses.parquet", index=False)

    address_data.head()

def split_collisions():
    # Load collisions from parquet file
    collisions = gpd.read_parquet(f"{CONTAINER_PATH}/data/processed/collisions.parquet")

    # Extracting address collisions
    address_collisions = collisions[collisions['stname1'].str.match(address_pattern, na=False) | collisions['stname2'].str.match(address_pattern, na=False)]

    # Extracting intersection collisions
    intersection_collisions = collisions[~collisions['collision_id'].isin(address_collisions['collision_id'])]

    # Saving to parquet files for next step
    intersection_collisions.to_parquet(f"{CONTAINER_PATH}/data/processed/intersection_collisions.parquet", index=False)
    address_collisions.to_parquet(f"{CONTAINER_PATH}/data/processed/address_collisions.parquet", index=False)

def geocode_collisions():
    # Load the collision data
    intersection_collisions = gpd.read_parquet(f"{CONTAINER_PATH}/data/processed/intersection_collisions.parquet")
    address_collisions = gpd.read_parquet(f"{CONTAINER_PATH}/data/processed/address_collisions.parquet")

    # Load the feature data
    intersections = gpd.read_parquet(f"{CONTAINER_PATH}/data/processed/intersections.parquet")
    addresses = gpd.read_parquet(f"{CONTAINER_PATH}/data/processed/addresses.parquet")

    # Geocoding intersection collisions using spatial join
    geocoded_intersection_collisions = gpd.sjoin_nearest(intersection_collisions, intersections, how="left", distance_col="distance")

    # Geocoding address collisions using spatial join
    geocoded_address_collisions = gpd.sjoin_nearest(address_collisions, addresses, how="left", distance_col="distance")

    # Saving geocoded collisions to parquet files for next step
    geocoded_intersection_collisions.to_parquet(f"{CONTAINER_PATH}/data/processed/geocoded_intersection_collisions.parquet", index=False)
    geocoded_address_collisions.to_parquet(f"{CONTAINER_PATH}/data/processed/geocoded_address_collisions.parquet", index=False)


def compute_match_scores():
    # Load geocoded collision data
    geocoded_intersection_collisions = gpd.read_parquet(f"{CONTAINER_PATH}/data/processed/geocoded_intersection_collisions.parquet")
    geocoded_address_collisions = gpd.read_parquet(f"{CONTAINER_PATH}/data/processed/geocoded_address_collisions.parquet")

    # Normalize location descriptions
    geocoded_intersection_collisions['location_description'] = (geocoded_intersection_collisions['stname1'] + " " + geocoded_intersection_collisions['stname2']).str.lower()

    geocoded_intersection_collisions['description'] = geocoded_intersection_collisions['description'].str.lower().str.replace("/", "")

    geocoded_address_collisions['stname2'] = geocoded_address_collisions['stname2'].fillna("")

    geocoded_address_collisions['location_description'] = (geocoded_address_collisions['stname1'] + " " + geocoded_address_collisions['stname2']).str.lower()

    geocoded_address_collisions['description'] = geocoded_address_collisions['description'].str.lower()

    # Compute match score for intersection collisions
    intersection_scores = process.cdist(geocoded_intersection_collisions['location_description'], geocoded_intersection_collisions['description'], scorer=fuzz.token_set_ratio)
    geocoded_intersection_collisions['match_score'] = intersection_scores.diagonal()

    # Compute match score for address collisions
    address_scores = process.cdist(geocoded_address_collisions['location_description'], geocoded_address_collisions['description'], scorer=fuzz.token_set_ratio)
    geocoded_address_collisions['match_score'] = address_scores.diagonal()

    # Saving the final geocoded collisions with match scores to parquet files
    geocoded_intersection_collisions.to_parquet(f"{CONTAINER_PATH}/data/processed/final_geocoded_intersection_collisions.parquet", index=False)
    geocoded_address_collisions.to_parquet(f"{CONTAINER_PATH}/data/processed/final_geocoded_address_collisions.parquet", index=False)

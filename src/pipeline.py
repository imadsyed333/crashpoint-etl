import json
import os
import urllib.request
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from rapidfuzz import fuzz, process
from shapely.geometry import shape

address_pattern = (
    r"^(?!.*\b(?:hwy|highway|ramp|expy|expressway|xwy|gardiner)\b)"
    r"\d{1,5}[a-z]?(?:\s+[a-z0-9'./()-]+){1,6}$"
)

DATA_DIR = Path(os.environ.get("DATA_DIR", Path(__file__).resolve().parents[1] / "data"))

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

    address_collisions = collisions[collisions['stname1'].str.match(address_pattern, case=False, na=False) | collisions['stname2'].str.match(address_pattern, case=False, na=False)]

    intersection_collisions = collisions[~collisions['collision_id'].isin(address_collisions['collision_id'])]

    intersection_collisions.to_parquet(processed_file("intersection_collisions.parquet"), index=False)
    address_collisions.to_parquet(processed_file("address_collisions.parquet"), index=False)


def _top_k(scores, k):
    if k == scores.shape[1]:
        order = np.argsort(-scores, axis=1)
        return order, np.take_along_axis(scores, order, axis=1)
    part = np.argpartition(scores, -k, axis=1)[:, -k:]
    part_scores = np.take_along_axis(scores, part, axis=1)
    order = np.argsort(-part_scores, axis=1)
    return np.take_along_axis(part, order, axis=1), np.take_along_axis(part_scores, order, axis=1)


def _containment_top(unique_queries, choices_arr, k):
    postings = {}
    for i, text in enumerate(choices_arr):
        for tok in set(text.split()):
            postings.setdefault(tok, []).append(i)
    n = len(choices_arr)
    top_idx = np.zeros((len(unique_queries), k), dtype=np.intp)
    top_score = np.zeros((len(unique_queries), k), dtype=np.float32)
    for qi, query in enumerate(unique_queries):
        tokens = set(query.split())
        if not tokens:
            continue
        counts = np.zeros(n, dtype=np.int16)
        for tok in tokens:
            idx = postings.get(tok)
            if idx:
                counts[idx] += 1
        if counts.max() == 0:
            continue
        picked, picked_scores = _top_k(counts.astype(np.float32)[None, :] / len(tokens), k)
        top_idx[qi] = picked[0]
        top_score[qi] = picked_scores[0]
    return top_idx, top_score


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

    k = min(5, len(choices_arr))
    top_idx = np.empty((len(unique_queries), k), dtype=np.intp)
    top_score = np.empty((len(unique_queries), k), dtype=np.float32)
    # ponytail: cap cdist matrix at ~128MB float32; raise if runners have more RAM
    batch = max(1, min(len(unique_queries), 32_000_000 // max(len(choices_arr), 1)))
    choices_list = choices_arr.tolist()
    for start in range(0, len(unique_queries), batch):
        chunk = unique_queries[start:start + batch].tolist()
        scores = process.cdist(chunk, choices_list, scorer=fuzz.token_sort_ratio, dtype=np.float32, workers=-1)
        top_idx[start:start + batch], top_score[start:start + batch] = _top_k(scores, k)

    rows = np.arange(len(collisions))
    feat_geoms = features.geometry.to_numpy()
    points = collisions.geometry.to_numpy()

    cand_idx = top_idx[inverse]
    cand_score = top_score[inverse]
    dist = shapely.distance(points[:, None], feat_geoms[cand_idx])
    winner = dist.argmin(axis=1)
    sort_best = cand_idx[rows, winner]
    sort_dist = dist[rows, winner]

    cont_idx, cont_score = _containment_top(unique_queries, choices_arr, k)
    cont_cand = cont_idx[inverse]
    cont_scores = cont_score[inverse]
    cont_dist = shapely.distance(points[:, None], feat_geoms[cont_cand])
    # zeros are ties, not matches; drop them so they can't win on distance
    cont_dist = np.where(cont_scores > 0, cont_dist, np.inf)
    cont_win = cont_dist.argmin(axis=1)
    cont_best = cont_cand[rows, cont_win]
    cont_best_dist = cont_dist[rows, cont_win]

    use_cont = cont_best_dist < sort_dist
    best_idx = np.where(use_cont, cont_best, sort_best)
    similarity = cand_score[rows, winner].copy()
    for i in np.flatnonzero(use_cont):
        similarity[i] = fuzz.token_sort_ratio(queries[i], choices_arr[best_idx[i]])

    matched = features.iloc[best_idx].reset_index(drop=True)

    out = collisions.copy()
    out["location_description"] = location_description.to_numpy()
    out["feature_id"] = matched["feature_id"].to_numpy()
    out["description"] = choices_arr[best_idx]
    out["type"] = matched["type"].to_numpy()
    out["similarity_score"] = similarity
    out["distance"] = shapely.distance(out.geometry.values, matched.geometry.values)
    match_ll = gpd.GeoSeries(matched.geometry.to_numpy(), crs=features.crs).to_crs(4326)
    out["match_latitude"] = match_ll.y.to_numpy()
    out["match_longitude"] = match_ll.x.to_numpy()
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

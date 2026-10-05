import os
import socket

import pandas as pd
import pydeck as pdk
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

COLUMNS = [
    "collision_id", "stname1", "stname2", "stname3",
    "latitude", "longitude", "match_latitude", "match_longitude",
    "description", "type", "similarity_score", "distance",
]


def engine_url(database_url):
    url = make_url(database_url)
    # ponytail: "postgis" only resolves on the compose network. Host-side Streamlit
    # uses the published port. No-op when that name already resolves.
    if url.host == "postgis":
        try:
            socket.getaddrinfo(url.host, url.port or 5432)
        except socket.gaierror:
            url = url.set(host="127.0.0.1")
    return url


@st.cache_data(show_spinner="Loading collisions")
def load_collisions(database_url):
    cols = ", ".join(COLUMNS)
    return pd.read_sql(f"SELECT {cols} FROM geocoded_collisions", create_engine(engine_url(database_url)))


def main():
    st.set_page_config(page_title="CrashPoint", layout="wide")
    st.title("Geocoded collisions")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        st.error("DATABASE_URL is required. Point it at the Postgres database that holds geocoded_collisions.")
        st.stop()

    try:
        collisions = load_collisions(database_url)
    except Exception as exc:
        st.error(f"Could not read geocoded_collisions ({exc}). Run the ETL so the table exists.")
        st.stop()
    collisions["similarity_score"] = collisions["similarity_score"].round(1)
    collisions["distance"] = collisions["distance"].round(1)

    kind = st.sidebar.selectbox("Match type", ["all", "intersection", "address"])
    min_score = st.sidebar.slider("Minimum similarity", 0, 100, 0)
    view = collisions
    if kind != "all":
        view = view[view["type"] == kind]
    view = view[view["similarity_score"] >= min_score]

    st.sidebar.caption(f"{len(view):,} collisions")

    if view.empty:
        st.info("No collisions match these filters.")
        return

    deck = pdk.Deck(
        initial_view_state=pdk.ViewState(
            latitude=view["latitude"].mean(),
            longitude=view["longitude"].mean(),
            zoom=11,
        ),
        layers=[
            pdk.Layer(
                "LineLayer",
                view,
                get_source_position=["longitude", "latitude"],
                get_target_position=["match_longitude", "match_latitude"],
                get_color=[140, 140, 140],
                get_width=2,
                pickable=True,
            ),
            pdk.Layer(
                "ScatterplotLayer",
                view,
                get_position=["longitude", "latitude"],
                get_fill_color=[200, 40, 40],
                get_radius=40,
                pickable=True,
            ),
            pdk.Layer(
                "ScatterplotLayer",
                view,
                get_position=["match_longitude", "match_latitude"],
                get_fill_color=[30, 90, 200],
                get_radius=30,
                pickable=True,
            ),
        ],
        tooltip={
            "html": (
                "<b>{stname1} {stname2}</b><br/>"
                "Match: {description}<br/>"
                "Type: {type}<br/>"
                "Similarity: {similarity_score}<br/>"
                "Distance: {distance} m"
            ),
        },
    )
    st.pydeck_chart(deck, width="stretch", height=600)
    st.caption("Red is the collision. Blue is the matched intersection or address.")
    st.dataframe(view, hide_index=True, width="stretch")


main()

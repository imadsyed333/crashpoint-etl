import math
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


def apply_filters(collisions, kind, min_score, max_distance, query):
    view = collisions
    if kind != "all":
        view = view[view["type"] == kind]
    view = view[(view["similarity_score"] >= min_score) & (view["distance"] <= max_distance)]
    query = query.strip()
    if query:
        text = (
            view["collision_id"].astype(str) + " "
            + view["stname1"].fillna("") + " "
            + view["stname2"].fillna("") + " "
            + view["stname3"].fillna("") + " "
            + view["description"].fillna("")
        ).str.lower()
        view = view[text.str.contains(query.lower(), regex=False)]
    return view


def map_zoom(view):
    span = max(
        float(view["latitude"].max() - view["latitude"].min()),
        float(view["longitude"].max() - view["longitude"].min()),
        1e-4,
    )
    # ponytail: log2(900/span) fits the span on an ~800px map; ignores widget width. Clamp 10–16.
    return min(16, max(10, math.log2(900 / span)))


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
    dmax = float(collisions["distance"].max())
    max_distance = st.sidebar.slider("Maximum distance (m)", 0.0, dmax, dmax) if dmax > 0 else dmax
    query = st.sidebar.text_input("Search collision, intersection, or address")
    view = apply_filters(collisions, kind, min_score, max_distance, query)

    st.sidebar.caption(f"{len(view):,} collisions")

    if view.empty:
        st.info("No collisions match these filters.")
        return

    # ponytail: deck.gl parks .deck-widgets-root as an unsized sibling of the canvas,
    # so the hover popup is shifted by that gap and clipped. Pin the root to the map.
    st.markdown(
        """
        <style>
        .deck-widgets-root {
            position: absolute;
            inset: 0;
            pointer-events: none;
            z-index: 2;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    deck = pdk.Deck(
        initial_view_state=pdk.ViewState(
            latitude=view["latitude"].mean(),
            longitude=view["longitude"].mean(),
            zoom=map_zoom(view),
        ),
        layers=[
            pdk.Layer(
                "LineLayer",
                view,
                get_source_position=["longitude", "latitude"],
                get_target_position=["match_longitude", "match_latitude"],
                get_color=[40, 160, 60],
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


if __name__ == "__main__":
    main()

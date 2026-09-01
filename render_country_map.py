"""Render a static Equal Earth world map of countries where alumni have taken a
course.

A country is filled blue if at least one person with an ``enrollments`` row has
that country on their ``people.country`` field (the same logic as the dashboard's
``/api/enrollments-by-country`` endpoint); every other country is white. Country
borders come from the app's own ``static/world.json`` and are drawn in light
gray. No title, legend, graticule or axis is added.

Usage::

    python render_country_map.py            # uses $CRM_DB, else crm.db, else test.db
    CRM_DB=test.db python render_country_map.py

Requires ``matplotlib`` and ``cartopy`` (see requirements.txt / environment.yml).
"""

import json
import os
import sqlite3
import sys
import warnings

import matplotlib

matplotlib.use("Agg")
import cartopy.crs as ccrs  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from shapely.geometry import shape  # noqa: E402

# cartopy emits a flood of RuntimeWarnings while clipping polygons to the
# Equal Earth boundary; the output is correct, so silence the noise.
warnings.filterwarnings("ignore", category=RuntimeWarning, module="shapely")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WORLD_JSON = os.path.join(BASE_DIR, "static", "world.json")
OUT_PNG = os.path.join(BASE_DIR, "static", "alumni_course_map.png")
OUT_SVG = os.path.join(BASE_DIR, "static", "alumni_course_map.svg")

COURSE_FILL = "#9ecae1"
OTHER_FILL = "white"
BORDER_COLOR = "#f0f0f0"
BORDER_WIDTH = 0.4
FRAME_COLOR = "darkgray"
FRAME_WIDTH = 0.8
FRAME_ALPHA = 0.5

# Countries to force white regardless of CRM data (normalised NAME_EN).
EXCLUDE = {"pakistan"}

# Feature properties tried (in order) when matching a CRM country string.
NAME_PROPS = (
    "NAME_EN",
    "NAME",
    "NAME_LONG",
    "ADMIN",
    "FORMAL_EN",
    "SOVEREIGNT",
    "GEOUNIT",
    "BRK_NAME",
)
ISO_PROPS = ("ISO_A2", "ISO_A3", "ISO_A2_EH", "ISO_A3_EH", "ADM0_A3", "POSTAL")

# CRM spellings that do not match any Natural Earth name field on a feature.
# key: normalised CRM string -> value: normalised name that IS on a feature.
ALIASES = {
    "usa": "united states of america",
    "us": "united states of america",
    "u.s.a.": "united states of america",
    "u.s.": "united states of america",
    "united states": "united states of america",
    "america": "united states of america",
    "uk": "united kingdom",
    "u.k.": "united kingdom",
    "great britain": "united kingdom",
    "england": "united kingdom",
    "scotland": "united kingdom",
    "wales": "united kingdom",
    "czech republic": "czechia",
    "cote d'ivoire": "ivory coast",
    "côte d'ivoire": "ivory coast",
    "burma": "myanmar",
    "swaziland": "eswatini",
    "cape verde": "cabo verde",
    "east timor": "timor-leste",
    "macedonia": "north macedonia",
    "the netherlands": "netherlands",
    "holland": "netherlands",
    "uae": "united arab emirates",
    "u.a.e.": "united arab emirates",
    "congo": "republic of the congo",
    "dr congo": "democratic republic of the congo",
    "drc": "democratic republic of the congo",
    "democratic republic of congo": "democratic republic of the congo",
    # non-English country names seen in the CRM data
    "alemania": "germany",
    "hrvatska": "croatia",
    "italia": "italy",
    "jepang": "japan",
    "espana": "spain",
    "españa": "spain",
}


def norm(value):
    """Normalise a country string for comparison."""
    return " ".join(str(value).strip().lower().split()) if value else ""


def resolve_db_path():
    env = os.environ.get("CRM_DB")
    if env:
        return env
    crm = os.path.join(BASE_DIR, "crm.db")
    if os.path.exists(crm):
        return crm
    return os.path.join(BASE_DIR, "test.db")  # fallback: fake data shipped in repo


def fetch_course_countries(db_path):
    if not os.path.exists(db_path):
        sys.exit(f"Database not found: {db_path}")
    con = sqlite3.connect(db_path)
    try:
        rows = con.execute(
            """SELECT DISTINCT p.country
                 FROM enrollments e
                 JOIN people p ON p.id = e.person_id
                WHERE p.country IS NOT NULL AND p.country != ''"""
        ).fetchall()
    finally:
        con.close()
    return sorted({r[0].strip() for r in rows if r[0] and r[0].strip()})


def build_feature_index(features):
    """Map every normalised name/ISO key on a feature to that feature's index."""
    index = {}
    for i, feat in enumerate(features):
        props = feat.get("properties", {})
        keys = set()
        for prop in NAME_PROPS:
            keys.add(norm(props.get(prop)))
        for prop in ISO_PROPS:
            val = props.get(prop)
            if val and str(val) not in ("-99", "-1"):
                keys.add(norm(val))
        for key in keys:
            if key:
                index.setdefault(key, i)
    return index


def match_countries(crm_countries, index):
    """Return (set_of_highlighted_feature_indices, list_of_unmatched_names)."""
    highlighted = set()
    unmatched = []
    for name in crm_countries:
        key = norm(name)
        target = index.get(key)
        if target is None and key in ALIASES:
            target = index.get(ALIASES[key])
        if target is None:
            unmatched.append(name)
        else:
            highlighted.add(target)
    return highlighted, unmatched


def main():
    db_path = resolve_db_path()
    print(f"Database:        {db_path}")

    crm_countries = fetch_course_countries(db_path)
    print(f"CRM countries:  {len(crm_countries)} -> {', '.join(crm_countries) or '(none)'}")

    with open(WORLD_JSON, encoding="utf-8") as fh:
        features = json.load(fh)["features"]

    index = build_feature_index(features)
    highlighted, unmatched = match_countries(crm_countries, index)

    excluded = {index[name] for name in EXCLUDE if name in index}
    dropped = highlighted & excluded
    highlighted -= excluded

    print(f"Highlighted:    {len(highlighted)} countries")
    if dropped:
        names = sorted(features[i]["properties"].get("NAME_EN", "?") for i in dropped)
        print(f"EXCLUDED:       {', '.join(names)}")
    if unmatched:
        print(f"UNMATCHED:      {', '.join(unmatched)}")

    course_geoms, other_geoms = [], []
    for i, feat in enumerate(features):
        props = feat.get("properties", {})
        if norm(props.get("NAME_EN")) == "antarctica" or props.get("CONTINENT") == "Antarctica":
            continue
        geom = shape(feat["geometry"])
        (course_geoms if i in highlighted else other_geoms).append(geom)

    fig = plt.figure(figsize=(16, 8))
    ax = plt.axes(projection=ccrs.EqualEarth())
    ax.set_global()
    ax.patch.set_visible(False)  # no background rectangle behind the globe

    ax.add_geometries(
        other_geoms,
        crs=ccrs.PlateCarree(),
        facecolor=OTHER_FILL,
        edgecolor=BORDER_COLOR,
        linewidth=BORDER_WIDTH,
    )
    ax.add_geometries(
        course_geoms,
        crs=ccrs.PlateCarree(),
        facecolor=COURSE_FILL,
        edgecolor=BORDER_COLOR,
        linewidth=BORDER_WIDTH,
    )

    # Draw the Equal Earth map boundary; hide any rectangular axis spines/ticks.
    for name, spine in ax.spines.items():
        if name == "geo":
            spine.set_visible(True)
            spine.set_edgecolor(FRAME_COLOR)
            spine.set_linewidth(FRAME_WIDTH)
            spine.set_alpha(FRAME_ALPHA)
        else:
            spine.set_visible(False)
    outline = getattr(ax, "outline_patch", None)  # cartopy < 0.18 fallback
    if outline is not None:
        outline.set_edgecolor(FRAME_COLOR)
        outline.set_linewidth(FRAME_WIDTH)
        outline.set_alpha(FRAME_ALPHA)
    ax.set_xticks([])
    ax.set_yticks([])

    fig.savefig(OUT_PNG, dpi=200, bbox_inches="tight", facecolor="white")
    fig.savefig(OUT_SVG, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    print(f"Wrote:          {OUT_PNG}")
    print(f"Wrote:          {OUT_SVG}")


if __name__ == "__main__":
    main()

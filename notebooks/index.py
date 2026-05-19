from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent
DATA_DIR = PROJECT_ROOT / "data" / "dashboard"

PAGE_TOTAL = "Vue globale"
PAGE_MISSIONS = "Missions et transports"
PAGE_EQUIPMENT = "Materiel informatique"
LONG_HAUL = "Avion long-courrier"

TEXT = "#111827"
GRID = "#E2E8F0"
BLUE = "#2563EB"
BLUE_DARK = "#1D4ED8"
BLUE_DEEP = "#1E40AF"
BLUE_LIGHT = "#60A5FA"
BLUE_SOFT = "#DBEAFE"
BLUE_PALE = "#CFEFE5"
SKY = "#38BDF8"
TEAL = "#0F766E"
TEAL_LIGHT = "#2DD4BF"
TEAL_PALE = "#F0FDFA"
SLATE = "#475569"

COLORWAY = [BLUE, TEAL, BLUE_LIGHT, TEAL_LIGHT, BLUE_DARK, SKY, BLUE_DEEP, SLATE]
SOURCE_COLORS = {"Missions": BLUE, "Materiel informatique": TEAL_LIGHT}
STATUS_COLORS = {"Facteur connu": TEAL, "Facteur manquant": SLATE}
IMPACT_SCALE = [BLUE_PALE, TEAL_LIGHT, BLUE_DARK]
HEAT_SCALE = [GRID, BLUE_PALE, BLUE_DARK]
MODEL_SCALE = [TEAL_PALE, TEAL_LIGHT, BLUE_DEEP]

CITY_COORDINATES = {
    "Alger": (36.7538, 3.0588),
    "Auckland": (-36.8509, 174.7645),
    "Berlin": (52.52, 13.405),
    "Bogota": (4.711, -74.0721),
    "Bordeaux": (44.8378, -0.5792),
    "Buenos Aires": (-34.6037, -58.3816),
    "Compi\u010dgne": (49.417, 2.8261),
    "Duba\u010f": (25.2048, 55.2708),
    "Helsinki": (60.1699, 24.9384),
    "Lille": (50.6292, 3.0573),
    "Lima": (-12.0464, -77.0428),
    "London": (51.5072, -0.1276),
    "Los Angeles": (34.0522, -118.2437),
    "Marseille": (43.2965, 5.3698),
    "Melbourne": (-37.8136, 144.9631),
    "Mexico": (19.4326, -99.1332),
    "Montreal": (45.5017, -73.5673),
    "New-York": (40.7128, -74.006),
    "Osaka": (34.6937, 135.5023),
    "Oslo": (59.9139, 10.7522),
    "Paris": (48.8566, 2.3522),
    "Pekin": (39.9042, 116.4074),
    "Rabat": (34.0209, -6.8416),
    "Rio de Janeiro": (-22.9068, -43.1729),
    "Sao Paulo": (-23.5558, -46.6396),
    "Shanghai": (31.2304, 121.4737),
    "Sidney": (-33.8688, 151.2093),
    "Stockholm": (59.3293, 18.0686),
    "Tokyo": (35.6762, 139.6503),
    "Tunis": (36.8065, 10.1815),
    "Vancouver": (49.2827, -123.1207),
    "Washington": (38.9072, -77.0369),
    "Wellington": (-41.2924, 174.7787),
}


st.set_page_config(
    page_title="Dashboard carbone BGES",
    layout="wide",
)

st.markdown(
    """
    <style>
    :root {
        --text: #111827;
        --muted: #64748B;
        --line: #E2E8F0;
        --surface: #FFFFFF;
        --soft: #F8FAFC;
        --primary: #2563EB;
        --primary-dark: #1D4ED8;
        --primary-soft: #DBEAFE;
        --primary-pale: #EFF6FF;
    }
    .stApp {
        background:
            linear-gradient(180deg, #F8FAFC 0%, #FFFFFF 32%),
            #FFFFFF;
    }
    .block-container {
        max-width: 1440px;
        padding-top: 1.4rem;
        padding-bottom: 2.4rem;
    }
    [data-testid="stSidebar"] {
        background: #F3F5F9;
        border-right: 1px solid #E2E8F0;
        box-shadow: 8px 0 24px rgba(15, 23, 42, 0.04);
    }
    [data-testid="stSidebar"] [data-testid="stSidebarContent"] {
        padding: 1.15rem 0.85rem;
    }
    [data-testid="stSidebar"] [data-testid="stSidebarContent"]::before {
        content: "BGES";
        display: block;
        color: var(--primary);
        font-size: 1.65rem;
        font-weight: 900;
        letter-spacing: 0;
        line-height: 1;
        margin: 0.2rem 0 1rem 0;
    }
    [data-testid="stSidebar"] * {
        color: #334155;
    }
    [data-testid="stSidebar"] label,
    [data-testid="stSidebar"] p {
        color: #475569;
        font-size: 0.86rem;
    }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #111827;
    }
    [data-testid="stSidebar"] hr {
        border-color: #E2E8F0;
        margin: 0.8rem 0;
    }
    [data-testid="stSidebar"] a {
        border-radius: 7px;
        transition: background 120ms ease, color 120ms ease;
    }
    [data-testid="stSidebar"] a:hover {
        background: var(--primary-pale);
        color: var(--primary-dark) !important;
    }
    [data-testid="stSidebar"] a[aria-current="page"],
    [data-testid="stSidebar"] [aria-current="page"],
    [data-testid="stSidebar"] a[data-active="true"] {
        background: var(--primary-soft) !important;
        color: var(--primary-dark) !important;
        font-weight: 800;
    }
    [data-testid="stSidebar"] a[aria-current="page"] *,
    [data-testid="stSidebar"] [aria-current="page"] *,
    [data-testid="stSidebar"] a[data-active="true"] * {
        color: var(--primary-dark) !important;
        font-weight: 800;
    }
    [data-testid="stSidebar"] [data-baseweb="tag"] {
        background: var(--primary-soft);
        border-radius: 6px;
        color: var(--primary-dark);
    }
    [data-testid="stSidebar"] [data-baseweb="select"] > div,
    [data-testid="stSidebar"] input,
    [data-testid="stSidebar"] [data-baseweb="input"] {
        background: #FFFFFF;
        border-color: #CBD5E1;
        border-radius: 7px;
    }
    [data-testid="stSidebar"] [data-baseweb="select"] > div:focus-within,
    [data-testid="stSidebar"] [data-baseweb="input"]:focus-within {
        border-color: var(--primary) !important;
        box-shadow: 0 0 0 1px var(--primary) !important;
    }
    [data-testid="stSidebar"] [data-testid="stCheckbox"] {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 7px;
        padding: 0.25rem 0.55rem;
    }
    [data-testid="stSidebar"] [data-testid="stCheckbox"] svg {
        color: var(--primary) !important;
        fill: var(--primary) !important;
    }
    [data-testid="stSidebar"] [data-testid="stCheckbox"] [data-checked="true"],
    [data-testid="stSidebar"] [data-testid="stCheckbox"] [aria-checked="true"] {
        color: var(--primary) !important;
        background-color: var(--primary) !important;
        border-color: var(--primary) !important;
    }
    div[data-testid="stSegmentedControl"] button[aria-pressed="true"],
    div[data-testid="stSegmentedControl"] button[data-active="true"],
    button[aria-pressed="true"] {
        background: var(--primary) !important;
        border-color: var(--primary) !important;
        color: #FFFFFF !important;
        font-weight: 800;
    }
    div[data-testid="stSegmentedControl"] button[aria-pressed="true"] *,
    div[data-testid="stSegmentedControl"] button[data-active="true"] *,
    button[aria-pressed="true"] * {
        color: #FFFFFF !important;
    }
    div[data-testid="stSegmentedControl"] button:hover,
    button:hover {
        border-color: var(--primary) !important;
        color: var(--primary-dark) !important;
    }
    .sidebar-section {
        color: var(--primary-dark);
        font-size: 0.72rem;
        font-weight: 800;
        letter-spacing: 0.06em;
        margin: 0.65rem 0 0.35rem 0;
        text-transform: uppercase;
    }
    .sidebar-note {
        color: #64748B;
        font-size: 0.78rem;
        line-height: 1.35;
        margin-top: 0.3rem;
    }
    .site-count {
        display: inline-block;
        background: var(--primary-pale);
        border: 1px solid var(--primary-soft);
        border-radius: 999px;
        color: var(--primary-dark);
        font-size: 0.76rem;
        font-weight: 700;
        padding: 0.22rem 0.55rem;
        margin: 0.2rem 0 0.4rem 0;
    }
    h1 {
        color: var(--primary-dark);
        letter-spacing: 0;
        font-size: 2.05rem;
        margin-bottom: 0.15rem;
    }
    .page-kicker {
        color: var(--muted);
        font-size: 0.98rem;
        margin-bottom: 1rem;
    }
    .kpi-card {
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 0.9rem 1rem;
        background: rgba(255, 255, 255, 0.92);
        box-shadow: 0 14px 30px rgba(15, 23, 42, 0.06);
        min-height: 118px;
    }
    .kpi-label {
        color: var(--muted);
        font-size: 0.78rem;
        font-weight: 700;
        letter-spacing: 0.04em;
        text-transform: uppercase;
        margin-bottom: 0.45rem;
    }
    .kpi-value {
        color: var(--text);
        font-size: 1.65rem;
        font-weight: 800;
        line-height: 1.1;
        overflow-wrap: anywhere;
    }
    .kpi-sub {
        color: var(--muted);
        font-size: 0.88rem;
        margin-top: 0.45rem;
    }
    .section-title {
        color: var(--primary-dark);
        font-size: 1.05rem;
        font-weight: 800;
        margin: 1rem 0 0.45rem 0;
    }
    div[data-testid="stExpander"] {
        border: 1px solid var(--line);
        border-radius: 8px;
        background: #FFFFFF;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def read_table(table_name: str) -> pd.DataFrame:
    path = DATA_DIR / table_name
    if not path.exists():
        st.error(f"Table introuvable: {path}")
        st.stop()
    return pd.read_parquet(path)


@st.cache_data(show_spinner="Chargement du datawarehouse...")
def load_data() -> dict[str, pd.DataFrame]:
    tables = {
        "dim_date": read_table("dim_date"),
        "dim_staff": read_table("dim_staff"),
        "dim_equipment": read_table("dim_equipment"),
        "dim_transport_type": read_table("dim_transport_type"),
        "dim_city": read_table("dim_city"),
        "dim_trip": read_table("dim_trip"),
        "fact_mission": read_table("fact_mission"),
        "fact_equipment": read_table("fact_equipment"),
    }

    date_dim = tables["dim_date"][["SK_DATE", "DATE_ISO"]].copy()
    date_dim["DATE_ISO"] = pd.to_datetime(date_dim["DATE_ISO"])

    city = tables["dim_city"].copy()
    city_lookup = city[["SK_CITY", "CITY_NAME", "COUNTRY_ISO2"]].copy()
    city_coords = city_lookup["CITY_NAME"].map(CITY_COORDINATES)
    city_lookup["LAT"] = city_coords.map(
        lambda coords: coords[0] if isinstance(coords, tuple) else None
    )
    city_lookup["LON"] = city_coords.map(
        lambda coords: coords[1] if isinstance(coords, tuple) else None
    )
    staff = tables["dim_staff"].copy()
    staff_lookup = staff[
        ["SK_STAFF", "JOB_TITLE", "ACTIVITY_SECTOR", "SK_SITE"]
    ].merge(
        city_lookup.rename(
            columns={
                "SK_CITY": "SK_SITE",
                "CITY_NAME": "SITE_CITY",
                "COUNTRY_ISO2": "SITE_COUNTRY",
                "LAT": "SITE_LAT",
                "LON": "SITE_LON",
            }
        ),
        on="SK_SITE",
        how="left",
    )

    trip = tables["dim_trip"].copy()
    trip_lookup = (
        trip.merge(
            city_lookup.rename(
                columns={
                    "SK_CITY": "SK_CITY_ORIGIN",
                    "CITY_NAME": "ORIGIN_CITY",
                    "COUNTRY_ISO2": "ORIGIN_COUNTRY",
                    "LAT": "ORIGIN_LAT",
                    "LON": "ORIGIN_LON",
                }
            ),
            on="SK_CITY_ORIGIN",
            how="left",
        )
        .merge(
            city_lookup.rename(
                columns={
                    "SK_CITY": "SK_CITY_DESTINATION",
                    "CITY_NAME": "DESTINATION_CITY",
                    "COUNTRY_ISO2": "DESTINATION_COUNTRY",
                    "LAT": "DESTINATION_LAT",
                    "LON": "DESTINATION_LON",
                }
            ),
            on="SK_CITY_DESTINATION",
            how="left",
        )
        .assign(
            ROUTE=lambda df: df["ORIGIN_CITY"].fillna("?")
            + " -> "
            + df["DESTINATION_CITY"].fillna("?")
        )
    )

    mission = (
        tables["fact_mission"]
        .merge(date_dim, left_on="SK_DATE_MISSION", right_on="SK_DATE", how="left")
        .merge(staff_lookup, on="SK_STAFF", how="left")
        .merge(tables["dim_transport_type"], on="SK_TRANSPORT_TYPE", how="left")
        .merge(trip_lookup, on="SK_TRIP", how="left")
        .rename(columns={"DATE_ISO": "DATE"})
    )
    mission["CO2_KG"] = mission["CO2_IMPACT_KG"].fillna(0)
    mission["CO2_T"] = mission["CO2_KG"] / 1000
    mission["SOURCE"] = "Missions"
    mission["MONTH"] = mission["DATE"].dt.to_period("M").dt.to_timestamp()
    mission["IS_LONG_HAUL"] = mission["TRANSPORT_NAME"].eq(LONG_HAUL)
    multiplier = mission["ROUND_TRIP_FLG"].map({True: 2, False: 1}).fillna(1)
    mission["DISTANCE_AR_KM"] = mission["DISTANCE_KM"].fillna(0) * multiplier

    equipment = (
        tables["fact_equipment"]
        .merge(date_dim, left_on="SK_DATE_PURCHASE", right_on="SK_DATE", how="left")
        .merge(staff_lookup, on="SK_STAFF", how="left")
        .merge(tables["dim_equipment"], on="SK_EQUIPMENT", how="left")
        .rename(columns={"DATE_ISO": "DATE"})
    )
    equipment["CO2_KG"] = equipment["CO2_IMPACT_KG"].fillna(0)
    equipment["CO2_T"] = equipment["CO2_KG"] / 1000
    equipment["SOURCE"] = "Materiel informatique"
    equipment["MONTH"] = equipment["DATE"].dt.to_period("M").dt.to_timestamp()
    equipment["TYPE"] = equipment["TYPE"].fillna("Type inconnu")
    equipment["MODEL"] = equipment["MODEL"].fillna("Modele inconnu")
    equipment["HAS_CO2_FACTOR"] = equipment["CO2_IMPACT_KG"].notna()

    return {**tables, "staff": staff_lookup, "mission": mission, "equipment": equipment}


def fmt_tonnes(kg: float) -> str:
    return f"{kg / 1000:,.1f} tCO2e".replace(",", " ")


def fmt_kg(kg: float) -> str:
    return f"{kg:,.0f} kg".replace(",", " ")


def fmt_pct(value: float) -> str:
    if pd.isna(value):
        return "0.0%"
    return f"{value:.1%}"


def scaled_map_size(values: pd.Series) -> pd.Series:
    values = values.astype(float).clip(lower=0).fillna(0)
    max_value = values.max()
    if max_value <= 0:
        return pd.Series(100_000, index=values.index)
    return 80_000 + 620_000 * (values / max_value).pow(0.5)


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    value = hex_color.lstrip("#")
    return tuple(int(value[i : i + 2], 16) for i in (0, 2, 4))


def _rgb_to_hex(rgb: tuple[int, int, int]) -> str:
    return "#{:02x}{:02x}{:02x}".format(*rgb)


def _interpolate_color(start: str, end: str, ratio: float) -> str:
    start_rgb = _hex_to_rgb(start)
    end_rgb = _hex_to_rgb(end)
    rgb = tuple(
        round(start_channel + (end_channel - start_channel) * ratio)
        for start_channel, end_channel in zip(start_rgb, end_rgb, strict=True)
    )
    return _rgb_to_hex(rgb)


def scaled_map_color(values: pd.Series) -> pd.Series:
    values = values.astype(float).clip(lower=0).fillna(0)
    min_value = values.min()
    max_value = values.max()
    if max_value <= min_value:
        return pd.Series(BLUE, index=values.index)

    ratios = ((values - min_value) / (max_value - min_value)).pow(0.7)
    colors = []
    for ratio in ratios:
        if ratio <= 0.5:
            colors.append(_interpolate_color(BLUE_PALE, TEAL_LIGHT, ratio * 2))
        else:
            colors.append(_interpolate_color(TEAL_LIGHT, BLUE_DARK, (ratio - 0.5) * 2))
    return pd.Series(colors, index=values.index)


def site_emission_map_data(
    mission: pd.DataFrame, equipment: pd.DataFrame
) -> pd.DataFrame:
    cols = {
        "SITE_CITY": "LOCALISATION",
        "SITE_COUNTRY": "COUNTRY",
        "SITE_LAT": "LAT",
        "SITE_LON": "LON",
    }
    sources = []
    if not mission.empty:
        sources.append(mission[[*cols.keys(), "CO2_KG"]].rename(columns=cols))
    if not equipment.empty:
        sources.append(equipment[[*cols.keys(), "CO2_KG"]].rename(columns=cols))
    if not sources:
        return pd.DataFrame()

    map_data = (
        pd.concat(sources, ignore_index=True)
        .dropna(subset=["LAT", "LON"])
        .groupby(["LOCALISATION", "COUNTRY", "LAT", "LON"], as_index=False)[
            "CO2_KG"
        ]
        .sum()
        .sort_values("CO2_KG", ascending=False)
    )
    map_data["CO2_T"] = map_data["CO2_KG"] / 1000
    map_data["POINT_SIZE_M"] = scaled_map_size(map_data["CO2_KG"])
    map_data["COLOR"] = scaled_map_color(map_data["CO2_KG"])
    return map_data


def destination_emission_map_data(mission: pd.DataFrame) -> pd.DataFrame:
    if mission.empty:
        return pd.DataFrame()

    map_data = (
        mission[
            [
                "DESTINATION_CITY",
                "DESTINATION_COUNTRY",
                "DESTINATION_LAT",
                "DESTINATION_LON",
                "CO2_KG",
            ]
        ]
        .rename(
            columns={
                "DESTINATION_CITY": "LOCALISATION",
                "DESTINATION_COUNTRY": "COUNTRY",
                "DESTINATION_LAT": "LAT",
                "DESTINATION_LON": "LON",
            }
        )
        .dropna(subset=["LAT", "LON"])
        .groupby(["LOCALISATION", "COUNTRY", "LAT", "LON"], as_index=False)[
            "CO2_KG"
        ]
        .sum()
        .sort_values("CO2_KG", ascending=False)
    )
    map_data["CO2_T"] = map_data["CO2_KG"] / 1000
    map_data["POINT_SIZE_M"] = scaled_map_size(map_data["CO2_KG"])
    map_data["COLOR"] = scaled_map_color(map_data["CO2_KG"])
    return map_data


def render_emission_map(mission: pd.DataFrame, equipment: pd.DataFrame) -> None:
    section("Carte des emissions")
    mode = st.segmented_control(
        "Localisation",
        ["Sites organisationnels", "Destinations des missions"],
        default="Sites organisationnels",
    )
    map_data = (
        site_emission_map_data(mission, equipment)
        if mode == "Sites organisationnels"
        else destination_emission_map_data(mission)
    )

    if map_data.empty:
        st.info("Aucune coordonnee disponible pour la carte avec ces filtres.")
        return

    left, right = st.columns([0.72, 0.28])
    with left:
        st.map(
            map_data,
            latitude="LAT",
            longitude="LON",
            size="POINT_SIZE_M",
            color="COLOR",
            height=500,
        )
    with right:
        st.caption(
            "La taille et la couleur des points augmentent avec les emissions filtrees."
        )
        top_locations = map_data[["LOCALISATION", "CO2_T"]].head(10).copy()
        top_locations["CO2_T"] = top_locations["CO2_T"].map(
            lambda value: f"{value:,.1f}".replace(",", " ")
        )
        top_locations = top_locations.rename(
            columns={"LOCALISATION": "Ville", "CO2_T": "tCO2e"}
        )
        st.dataframe(top_locations, width="stretch", hide_index=True)


def page_header(title: str, caption: str) -> None:
    st.title(title)
    st.markdown(f"<div class='page-kicker'>{caption}</div>", unsafe_allow_html=True)


def section(title: str) -> None:
    st.markdown(f"<div class='section-title'>{title}</div>", unsafe_allow_html=True)


def plot(
    fig: go.Figure,
    *,
    height: int = 420,
    margin_t: int = 58,
    margin_b: int = 22,
) -> None:
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=12, r=12, t=margin_t, b=margin_b),
        font=dict(family="Arial", size=13, color=TEXT),
        title=dict(font=dict(size=16, color=TEXT), x=0.02, xanchor="left"),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(size=12),
        ),
        colorway=COLORWAY,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hoverlabel=dict(bgcolor="#0F172A", font_color="#F8FAFC"),
    )
    fig.update_xaxes(showgrid=True, gridcolor=GRID, zeroline=False, title_font_size=12)
    fig.update_yaxes(showgrid=False, zeroline=False, title_font_size=12)
    st.plotly_chart(fig, width="stretch")


def metric_row(metrics: list[tuple[str, str, str, str]]) -> None:
    cols = st.columns(len(metrics))
    for col, (label, value, subtext, color) in zip(cols, metrics, strict=True):
        col.markdown(
            f"""
            <div class="kpi-card" style="border-top: 4px solid {color};">
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{value}</div>
                <div class="kpi-sub">{subtext}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def filter_data(
    mission: pd.DataFrame, equipment: pd.DataFrame, staff: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    min_date = min(mission["DATE"].min(), equipment["DATE"].min()).date()
    max_date = max(mission["DATE"].max(), equipment["DATE"].max()).date()

    st.sidebar.markdown(
        "<div class='sidebar-section'>Filtres</div>",
        unsafe_allow_html=True,
    )
    selected_dates = st.sidebar.date_input(
        "Periode",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        start_date, end_date = selected_dates
    else:
        start_date, end_date = min_date, max_date

    site_options = sorted(
        set(mission["SITE_CITY"].dropna()).union(equipment["SITE_CITY"].dropna())
    )
    all_sites = st.sidebar.checkbox("Tous les sites", value=True)
    if all_sites:
        selected_sites = site_options
        st.sidebar.markdown(
            f"<span class='site-count'>{len(site_options)} sites actifs</span>",
            unsafe_allow_html=True,
        )
    else:
        selected_sites = st.sidebar.multiselect(
            "Sites a inclure",
            options=site_options,
            default=site_options[: min(3, len(site_options))],
            placeholder="Choisir un ou plusieurs sites",
        )
        if selected_sites:
            st.sidebar.markdown(
                f"<span class='site-count'>{len(selected_sites)} site(s) selectionne(s)</span>",
                unsafe_allow_html=True,
            )
        else:
            st.sidebar.markdown(
                "<div class='sidebar-note'>Aucun site selectionne.</div>",
                unsafe_allow_html=True,
            )

    st.sidebar.markdown(
        "<div class='sidebar-note'>Les graphiques se recalculent automatiquement avec ces filtres.</div>",
        unsafe_allow_html=True,
    )

    mission_filtered = mission[
        (mission["DATE"].dt.date >= start_date)
        & (mission["DATE"].dt.date <= end_date)
        & (mission["SITE_CITY"].isin(selected_sites))
    ].copy()
    equipment_filtered = equipment[
        (equipment["DATE"].dt.date >= start_date)
        & (equipment["DATE"].dt.date <= end_date)
        & (equipment["SITE_CITY"].isin(selected_sites))
    ].copy()
    staff_filtered = staff[staff["SITE_CITY"].isin(selected_sites)].copy()

    return mission_filtered, equipment_filtered, staff_filtered


def render_total_page_from_state() -> None:
    render_total_page(
        st.session_state["mission_filtered"],
        st.session_state["equipment_filtered"],
        st.session_state["staff_filtered"],
    )


def render_mission_page_from_state() -> None:
    render_mission_page(st.session_state["mission_filtered"])


def render_equipment_page_from_state() -> None:
    render_equipment_page(st.session_state["equipment_filtered"])


def build_navigation():
    return st.navigation(
        [
            st.Page(
                render_total_page_from_state,
                title=PAGE_TOTAL,
                url_path="vue-globale",
                default=True,
            ),
            st.Page(
                render_mission_page_from_state,
                title=PAGE_MISSIONS,
                url_path="missions-transports",
            ),
            st.Page(
                render_equipment_page_from_state,
                title=PAGE_EQUIPMENT,
                url_path="materiel-informatique",
            ),
        ],
        position="sidebar",
        expanded=True,
    )


def render_total_page(
    mission: pd.DataFrame, equipment: pd.DataFrame, staff: pd.DataFrame
) -> None:
    page_header(
        PAGE_TOTAL,
        "Synthese generale des emissions, de leur evolution et de leur intensite par site.",
    )

    mission_kg = mission["CO2_KG"].sum()
    equipment_kg = equipment["CO2_KG"].sum()
    total_kg = mission_kg + equipment_kg
    employee_count = int(staff["SK_STAFF"].nunique()) if not staff.empty else 0
    co2_per_employee_kg = total_kg / employee_count if employee_count else 0

    metric_row(
        [
            ("Total emissions", fmt_tonnes(total_kg), "Missions + materiel", BLUE),
            (
                "Missions",
                fmt_tonnes(mission_kg),
                f"{fmt_pct(mission_kg / total_kg if total_kg else 0)} du total",
                BLUE,
            ),
            (
                "Equipement",
                fmt_tonnes(equipment_kg),
                f"{fmt_pct(equipment_kg / total_kg if total_kg else 0)} du total",
                BLUE,
            ),
            (
                "CO2 / employe",
                fmt_tonnes(co2_per_employee_kg),
                f"{employee_count:,} employes couverts".replace(",", " "),
                BLUE,
            ),
        ]
    )

    render_emission_map(mission, equipment)

    source_df = pd.DataFrame(
        {
            "SOURCE": ["Missions", "Materiel informatique"],
            "CO2_KG": [mission_kg, equipment_kg],
            "CO2_T": [mission_kg / 1000, equipment_kg / 1000],
        }
    )

    mission_time = mission.groupby(["MONTH", "SOURCE"], as_index=False)[
        "CO2_KG"
    ].sum()
    equipment_time = equipment.groupby(["MONTH", "SOURCE"], as_index=False)[
        "CO2_KG"
    ].sum()
    time_df = pd.concat([mission_time, equipment_time], ignore_index=True)
    time_df["CO2_T"] = time_df["CO2_KG"] / 1000

    site_source = pd.concat(
        [
            mission.groupby(["SITE_CITY", "SOURCE"], as_index=False)["CO2_KG"].sum(),
            equipment.groupby(["SITE_CITY", "SOURCE"], as_index=False)["CO2_KG"].sum(),
        ],
        ignore_index=True,
    )
    staff_by_site = (
        staff.groupby("SITE_CITY", as_index=False)["SK_STAFF"]
        .nunique()
        .rename(columns={"SK_STAFF": "EMPLOYES"})
    )
    site_total = (
        site_source.groupby("SITE_CITY", as_index=False)["CO2_KG"].sum()
        .merge(staff_by_site, on="SITE_CITY", how="left")
        .fillna({"EMPLOYES": 0})
    )
    site_total["CO2_T"] = site_total["CO2_KG"] / 1000
    site_total["CO2_T_PAR_EMPLOYE"] = site_total["CO2_T"] / site_total[
        "EMPLOYES"
    ].mask(site_total["EMPLOYES"].eq(0))
    site_order = site_total.sort_values("CO2_T", ascending=False)["SITE_CITY"].tolist()
    site_total = site_total.set_index("SITE_CITY").loc[site_order].reset_index()

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=site_total["SITE_CITY"],
            y=site_total["CO2_T"],
            name="Total",
            marker_color=BLUE,
            hovertemplate="%{x}<br>%{y:.1f} tCO2e<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=site_total["SITE_CITY"],
            y=site_total["CO2_T_PAR_EMPLOYE"],
            name="Par employe",
            yaxis="y2",
            mode="lines+markers",
            line=dict(color=TEAL_LIGHT, width=3),
            marker=dict(size=9),
            hovertemplate="%{x}<br>%{y:.2f} tCO2e / employe<extra></extra>",
        )
    )
    fig.update_layout(
        title="Total par site et intensite par employe",
        xaxis_title="Site",
        yaxis=dict(title="tCO2e"),
        yaxis2=dict(
            title="tCO2e / employe",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
    )
    plot(fig, height=450)

    left, right = st.columns([0.50, 0.50])
    with left:
        fig = px.bar(
            source_df,
            x="SOURCE",
            y="CO2_T",
            color="SOURCE",
            title="Repartition missions / equipement",
            color_discrete_map=SOURCE_COLORS,
        )
        plot(fig, height=410)
    with right:
        fig = px.bar(
            time_df,
            x="MONTH",
            y="CO2_T",
            color="SOURCE",
            title="Emissions en fonction du temps",
            labels={"MONTH": "Mois", "CO2_T": "tCO2e", "SOURCE": ""},
            color_discrete_map=SOURCE_COLORS,
        )
        plot(fig, height=410)


def render_mission_page(mission: pd.DataFrame) -> None:
    page_header(
        PAGE_MISSIONS,
        "Focus sur les leviers de reduction: long-courrier, routes et motifs.",
    )

    if mission.empty:
        st.warning("Aucune mission disponible pour les filtres selectionnes.")
        return

    mission_kg = mission["CO2_KG"].sum()
    long = mission[mission["IS_LONG_HAUL"]].copy()
    long_kg = long["CO2_KG"].sum()
    long_share = long_kg / mission_kg if mission_kg else 0
    long_volume_share = len(long) / len(mission) if len(mission) else 0
    top_route = (
        long.groupby("ROUTE")["CO2_KG"].sum().sort_values(ascending=False).index[0]
        if not long.empty
        else "n/a"
    )

    metric_row(
        [
            ("Emissions missions", fmt_tonnes(mission_kg), "Base fact_mission", BLUE),
            (
                "Long-courrier",
                fmt_tonnes(long_kg),
                f"{fmt_pct(long_share)} du CO2 missions",
                BLUE,
            ),
            (
                "Volume long-courrier",
                f"{len(long):,}".replace(",", " "),
                f"{fmt_pct(long_volume_share)} des missions",
                BLUE,
            ),
            ("Route #1", top_route, "Plus gros gisement carbone", BLUE),
        ]
    )

    transport = (
        mission.groupby("TRANSPORT_NAME", as_index=False)
        .agg(
            CO2_KG=("CO2_KG", "sum"),
            MISSIONS=("NK_MISSION", "count"),
            KM=("DISTANCE_AR_KM", "sum"),
            CO2_FACTOR_KG_PER_KM=("CO2_FACTOR_KG_PER_KM", "mean"),
        )
        .sort_values("CO2_KG", ascending=False)
    )
    transport["CO2_T"] = transport["CO2_KG"] / 1000
    transport["PART_MISSIONS"] = transport["MISSIONS"] / transport["MISSIONS"].sum()
    transport["KG_PAR_KM"] = (
        transport["CO2_KG"] / transport["KM"].mask(transport["KM"].eq(0))
    ).fillna(transport["CO2_FACTOR_KG_PER_KM"])

    transport_chart = transport.sort_values("CO2_KG", ascending=False).copy()

    left, right = st.columns(2)
    with left:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=transport_chart["TRANSPORT_NAME"],
                y=transport_chart["CO2_T"],
                name="Emissions",
                marker_color=BLUE,
                text=transport_chart["CO2_T"].map(lambda value: f"{value:.1f} t"),
                textposition="inside",
                insidetextanchor="end",
                textfont=dict(color="#FFFFFF", size=11),
                hovertemplate="%{x}<br>%{y:.1f} tCO2e<extra></extra>",
            )
        )
        fig.update_layout(
            title="Emissions selon le transport",
            showlegend=False,
            xaxis_title="Transport",
            yaxis=dict(title="tCO2e"),
        )
        fig.update_xaxes(tickangle=-35)
        plot(fig, height=430, margin_b=86)

    with right:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=transport_chart["TRANSPORT_NAME"],
                y=transport_chart["PART_MISSIONS"],
                name="Taux d'utilisation",
                marker_color=BLUE,
                hovertemplate="%{x}<br>%{y:.1%} des missions<extra></extra>",
            )
        )
        fig.add_trace(
            go.Scatter(
                x=transport_chart["TRANSPORT_NAME"],
                y=transport_chart["KG_PAR_KM"],
                name="kg CO2e / km",
                yaxis="y2",
                mode="lines+markers",
                line=dict(color=TEAL_LIGHT, width=3),
                marker=dict(size=9),
                hovertemplate="%{x}<br>%{y:.3f} kg CO2e / km<extra></extra>",
            )
        )
        fig.update_layout(
            title="Intensite carbone et taux d'utilisation",
            xaxis_title="Transport",
            yaxis=dict(title="Taux d'utilisation", tickformat=".0%"),
            yaxis2=dict(
                title="kg CO2e / km",
                overlaying="y",
                side="right",
                showgrid=False,
                tickformat=".3f",
            ),
        )
        fig.update_xaxes(tickangle=-35)
        plot(fig, height=430, margin_b=86)

    heat = (
        long.groupby(["ORIGIN_CITY", "DESTINATION_CITY"], as_index=False)["CO2_KG"]
        .sum()
        .assign(CO2_T=lambda df: df["CO2_KG"] / 1000)
    )

    top_origins = heat.groupby("ORIGIN_CITY")["CO2_T"].sum().nlargest(8).index
    top_destinations = heat.groupby("DESTINATION_CITY")["CO2_T"].sum().nlargest(12).index
    heat = heat[
        heat["ORIGIN_CITY"].isin(top_origins)
        & heat["DESTINATION_CITY"].isin(top_destinations)
    ]
    fig = px.density_heatmap(
        heat,
        x="DESTINATION_CITY",
        y="ORIGIN_CITY",
        z="CO2_T",
        histfunc="sum",
        title="Matrice origine / destination long-courrier",
        labels={
            "DESTINATION_CITY": "Destination",
            "ORIGIN_CITY": "Origine",
            "CO2_T": "tCO2e",
        },
        color_continuous_scale=HEAT_SCALE,
    )
    fig.update_layout(coloraxis_colorbar=dict(title="tCO2e"))
    plot(fig, height=470)

    section("Motifs et details operationnels")
    mission_type = (
        mission.groupby("MISSION_TYPE", as_index=False)["CO2_KG"]
        .sum()
        .assign(CO2_T=lambda df: df["CO2_KG"] / 1000)
        .sort_values("CO2_T", ascending=False)
    )
    fig = px.pie(
        mission_type,
        names="MISSION_TYPE",
        values="CO2_T",
        hole=0.42,
        title="Emissions par motif de mission",
        color_discrete_sequence=COLORWAY,
    )
    fig.update_traces(
        textinfo="percent+label",
        hovertemplate="%{label}<br>%{value:.1f} tCO2e<extra></extra>",
    )
    plot(fig, height=430)

    with st.expander("Missions long-courrier les plus emettrices"):
        cols = [
            "NK_MISSION",
            "DATE",
            "SITE_CITY",
            "MISSION_TYPE",
            "ROUTE",
            "DISTANCE_AR_KM",
            "CO2_KG",
        ]
        st.dataframe(
            long.sort_values("CO2_KG", ascending=False)[cols].head(120),
            width="stretch",
            hide_index=True,
        )


def render_equipment_page(equipment: pd.DataFrame) -> None:
    page_header(
        PAGE_EQUIPMENT,
        "Analyse des achats, familles de materiel et qualite des facteurs CO2.",
    )

    if equipment.empty:
        st.warning("Aucun achat de materiel disponible pour les filtres selectionnes.")
        return

    known = equipment[equipment["HAS_CO2_FACTOR"]]
    missing = equipment[~equipment["HAS_CO2_FACTOR"]]
    equipment_kg = equipment["CO2_KG"].sum()
    avg_kg = known["CO2_KG"].mean() if len(known) else 0
    coverage = len(known) / len(equipment) if len(equipment) else 0
    top_type = (
        equipment.groupby("TYPE")["CO2_KG"].sum().sort_values(ascending=False).index[0]
        if len(equipment)
        else "n/a"
    )

    metric_row(
        [
            ("Emissions materiel", fmt_tonnes(equipment_kg), "Poste secondaire", BLUE),
            ("Achats analyses", f"{len(equipment):,}".replace(",", " "), top_type, BLUE),
            ("Moyenne par achat", fmt_kg(avg_kg), "Sur facteurs connus", BLUE),
            ("Couverture CO2", fmt_pct(coverage), f"{len(missing):,} inconnus".replace(",", " "), BLUE),
        ]
    )

    by_type = (
        equipment.groupby("TYPE", as_index=False)
        .agg(
            CO2_KG=("CO2_KG", "sum"),
            ACHATS=("NK_EQUIPMENT", "count"),
            COUVERTS=("HAS_CO2_FACTOR", "sum"),
        )
        .sort_values("CO2_KG", ascending=False)
    )
    by_type["CO2_T"] = by_type["CO2_KG"] / 1000
    by_type["COUVERTURE"] = by_type["COUVERTS"] / by_type["ACHATS"].mask(
        by_type["ACHATS"].eq(0)
    )
    by_type["KG_PAR_ACHAT"] = by_type["CO2_KG"] / by_type["COUVERTS"].mask(
        by_type["COUVERTS"].eq(0)
    )

    left, right = st.columns([0.54, 0.46])
    with left:
        fig = px.bar(
            by_type.head(14).sort_values("CO2_T"),
            y="TYPE",
            x="CO2_T",
            color="COUVERTURE",
            orientation="h",
            title="Familles de materiel les plus emettrices",
            labels={"TYPE": "", "CO2_T": "tCO2e", "COUVERTURE": "Couverture"},
            color_continuous_scale=IMPACT_SCALE,
        )
        fig.update_layout(coloraxis_colorbar=dict(title="Couverture"))
        plot(fig, height=500)
    with right:
        fig = px.scatter(
            by_type,
            x="ACHATS",
            y="KG_PAR_ACHAT",
            size="CO2_T",
            color="CO2_T",
            hover_name="TYPE",
            title="Volume d'achats vs intensite carbone",
            labels={
                "ACHATS": "Nombre d'achats",
                "KG_PAR_ACHAT": "kg CO2e / achat connu",
                "CO2_T": "tCO2e",
            },
            color_continuous_scale=IMPACT_SCALE,
        )
        plot(fig, height=500)

    by_model = (
        equipment.groupby(["TYPE", "MODEL"], as_index=False)
        .agg(CO2_KG=("CO2_KG", "sum"), ACHATS=("NK_EQUIPMENT", "count"))
        .assign(CO2_T=lambda df: df["CO2_KG"] / 1000)
        .sort_values("CO2_KG", ascending=False)
        .head(30)
    )
    coverage_type = (
        equipment.groupby(["TYPE", "HAS_CO2_FACTOR"], as_index=False)["NK_EQUIPMENT"]
        .count()
        .rename(columns={"NK_EQUIPMENT": "ACHATS"})
    )
    coverage_type["STATUT"] = coverage_type["HAS_CO2_FACTOR"].map(
        {True: "Facteur connu", False: "Facteur manquant"}
    )

    left, right = st.columns([0.5, 0.5])
    with left:
        fig = px.treemap(
            by_model,
            path=["TYPE", "MODEL"],
            values="CO2_T",
            color="ACHATS",
            title="Modeles qui concentrent l'impact",
            labels={"CO2_T": "tCO2e", "ACHATS": "Achats"},
            color_continuous_scale=MODEL_SCALE,
        )
        plot(fig, height=480)
    with right:
        fig = px.bar(
            coverage_type,
            x="TYPE",
            y="ACHATS",
            color="STATUT",
            title="Qualite des facteurs carbone par type",
            labels={"TYPE": "Type", "ACHATS": "Achats", "STATUT": ""},
            color_discrete_map=STATUS_COLORS,
        )
        plot(fig, height=480)

    monthly = equipment.groupby(["MONTH", "TYPE"], as_index=False)["CO2_KG"].sum()
    top_types = by_type.head(6)["TYPE"].tolist()
    monthly = monthly[monthly["TYPE"].isin(top_types)].assign(
        CO2_T=lambda df: df["CO2_KG"] / 1000
    )
    fig = px.area(
        monthly,
        x="MONTH",
        y="CO2_T",
        color="TYPE",
        title="Evolution mensuelle des principales familles",
        labels={"MONTH": "Mois", "CO2_T": "tCO2e", "TYPE": ""},
        color_discrete_sequence=COLORWAY,
    )
    plot(fig, height=430)

    if not missing.empty:
        with st.expander("Achats sans facteur carbone associe"):
            cols = ["NK_EQUIPMENT", "DATE", "SITE_CITY", "TYPE", "MODEL", "SK_EQUIPMENT"]
            st.dataframe(
                missing.sort_values("DATE", ascending=False)[cols].head(200),
                width="stretch",
                hide_index=True,
            )


def main() -> None:
    page = build_navigation()
    data = load_data()
    mission, equipment, staff = filter_data(
        data["mission"], data["equipment"], data["staff"]
    )
    st.sidebar.divider()
    st.sidebar.caption(f"Source: `{DATA_DIR.relative_to(PROJECT_ROOT)}`")

    if mission.empty and equipment.empty:
        st.warning("Aucune donnee disponible pour les filtres selectionnes.")
        return

    st.session_state["mission_filtered"] = mission
    st.session_state["equipment_filtered"] = equipment
    st.session_state["staff_filtered"] = staff
    page.run()


if __name__ == "__main__":
    main()

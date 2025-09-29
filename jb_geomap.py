import json
import pandas as pd
import streamlit as st
import pydeck as pdk
import numpy as np
from bisect import bisect_right

from snowflake.snowpark import Session

session = Session.builder.config("connection_name", "sel").getOrCreate()

def fmt_int(n: int) -> str:
    return f"{int(n):,}"

def fmt_pct(x: float, dp: int = 1) -> str:
    return f"{x:.{dp}f}%"

def fmt_gbp(x: float) -> str:
    return f"£{int(round(x)):,}"
    
# If you prefer compact units (k, M), use:
def fmt_gbp_compact(x: float) -> str:
    absx = abs(x)
    if absx >= 1_000_000:
        return f"£{x/1_000_000:.1f}M"
    if absx >= 1_000:
        return f"£{x/1_000:.1f}k"
    return f"£{int(round(x)):,}"


# Page configuration
# st.set_page_config(page_title="SE London Population Density", layout="wide")
st.title("South East London Health and Care Demographics Map")
# st.image("nhs_sel_logo.jpg", width=180 )


@st.cache_data
def load_population_data():
    active_areas = session.sql("""
WITH latest_addr AS (
  SELECT
    pa.person_id,
    pa.lsoa_2011,
    pa.imd_decile,
    pa.la_name
  FROM pipeline_prod.stg_gp__person_address pa
  WHERE active_record_flag = TRUE
    AND lsoa_2011 IS NOT NULL
    AND la_name IN ('Lambeth','Southwark','Lewisham','Bromley','Bexley','Greenwich')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY pa.person_id ORDER BY valid_from DESC) = 1
),
conditions AS (
  SELECT
    person_id,
    COUNT(DISTINCT definition_id) AS dx_count
  FROM data_lab_ai_centre.pipeline_prod.gld_diagnosis_prevalence
  WHERE prevalence_year = 2024
  GROUP BY person_id
),
utilisation AS (
  SELECT
    person_id,
    SUM(gp_appointment_count)     AS gp_appointment_count,
    SUM(unplanned_admission_days_sum) AS unplanned_admission_days,
    SUM(tariff_per_person_sum)    AS tariff
  FROM data_lab_ai_centre.pipeline_prod.gld_utilisation
  WHERE attendance_year = 2024
  GROUP BY person_id
),
pid as (
SELECT person_id, SK_PATIENT_ID
FROM pipeline_prod.BASE_DISCOVERY__PATIENT_PSEUDO_ID
),
ucp AS (
  SELECT
    "SK_PatientID" AS SK_PATIENTID,
    COUNT(*)       AS NUMBER_OF_UCPS
  FROM "Data_Store_Universal_Care_Plan".UCP."patient_key_information_ro"
  WHERE "care_plan_name"='Urgent Care Plan'
  GROUP BY "SK_PatientID"
)

SELECT
  a.lsoa_2011,
  a.imd_decile,
  a.la_name,

  COUNT(*) AS total_patients,  -- one row per person in latest_addr

  -- diagnoses
  SUM( IFF(COALESCE(c.dx_count,0) >= 1, 1, 0) )                AS patients_with_dx,
  ROUND( 100 * SUM(IFF(COALESCE(c.dx_count,0) >= 1, 1, 0))
            / NULLIF(COUNT(*),0), 2 )                          AS patients_with_dx_perc,
  SUM( COALESCE(c.dx_count,0) )                                AS total_dx_codes,  -- optional

  -- utilisation
  SUM( COALESCE(u.gp_appointment_count,0) )                    AS gp_appointment_count,
  SUM( COALESCE(u.unplanned_admission_days,0) )                AS unplanned_admission_days,
  ROUND(SUM(COALESCE(u.tariff,0) ),0)                          AS tariff,
  ROUND(SUM(COALESCE(u.tariff,0))/total_patients,0)            AS tariff_per_capita,
  ROUND(SUM(COALESCE(ucp.number_of_ucps,0) ),0)                AS patients_with_ucp
  
FROM latest_addr a
JOIN PID ON PID.person_id = a.person_id
LEFT JOIN conditions  c ON c.person_id = a.person_id
LEFT JOIN utilisation u ON u.person_id = a.person_id
LEFT JOIN UCP ucp on ucp.SK_PATIENTID=pid.SK_PATIENT_ID
GROUP BY a.lsoa_2011, a.imd_decile, a.la_name
ORDER BY patients_with_dx_perc ;
                               """).collect()
    return pd.DataFrame(active_areas)

# 5-step palettes
PALETTE_COUNTS = [  # blue -> red, no green
    (0, 0, 255),
    (64, 0, 191),
    (128, 0, 128),
    (191, 0, 64),
    (255, 0, 0),
]
PALETTE_IMD = [  # red -> green for deprivation
    (220, 0, 0),
    (255, 85, 0),
    (255, 170, 0),
    (120, 200, 0),
    (0, 160, 0),
]

def compute_quintile_edges(series: pd.Series) -> np.ndarray:
    """Return 6 edges for 5 quantile bins (q0..q5). Ensures strictly increasing."""
    s = pd.to_numeric(series, errors="coerce").astype(float).dropna()
    if s.empty:
        return np.array([0, 1, 2, 3, 4, 5], dtype=float)
    qs = s.quantile([0, 0.2, 0.4, 0.6, 0.8, 1.0]).to_numpy(dtype=float)
    # make strictly increasing to avoid flat bins
    eps = 1e-9
    for i in range(1, len(qs)):
        if qs[i] <= qs[i - 1]:
            qs[i] = qs[i - 1] + eps
    return qs

def color_from_quintiles(value: float, edges: np.ndarray, metric: str):
    """Map a numeric value to one of 5 discrete colors using the provided edges."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return [128, 128, 128, 220]
    idx = bisect_right(edges, float(value)) - 1   # 0..4
    idx = max(0, min(4, idx))
    r, g, b = (PALETTE_IMD[idx] if metric == "IMD_DECILE" else PALETTE_COUNTS[idx])
    return [int(r), int(g), int(b), 220]



@st.cache_data
def load_geojson():
    with open("sel_lsoa.geojson", "r") as f:
        geojson_data = json.load(f)
    return geojson_data


@st.cache_data
def create_geojson_with_population(highlight_metric="population"):
    population_df = load_population_data()
    geojson_data = load_geojson()

    # --- Ensure JSON-safe, math-friendly types ---
    # (Snowflake often returns NUMBER/DECIMAL -> Decimal)
    for col in ("IMD_DECILE","TOTAL_PATIENTS", "PATIENTS_WITH_DX","GP_APPOINTMENT_COUNT", "UNPLANNED_ADMISSION_DAYS","PATIENTS_WITH_UCP"):
        population_df[col] = population_df[col].astype(int)
    population_df["PATIENTS_WITH_DX_PERC"] = population_df["PATIENTS_WITH_DX_PERC"].astype(float)
    population_df["TARIFF"] = population_df["TARIFF"].astype(float)
    population_df["TARIFF_PER_CAPITA"] = population_df["TARIFF_PER_CAPITA"].astype(float)
    
    # Lookups
    TOTAL_PATIENTS_dict       = dict(zip(population_df["LSOA_2011"], population_df["TOTAL_PATIENTS"]))
    PATIENTS_WITH_DX_dict      = dict(zip(population_df["LSOA_2011"], population_df["PATIENTS_WITH_DX"]))
    PATIENTS_WITH_DX_PERC_dict = dict(zip(population_df["LSOA_2011"], population_df["PATIENTS_WITH_DX_PERC"]))
    IMD_DECILE_dict       = dict(zip(population_df["LSOA_2011"], population_df["IMD_DECILE"]))
    LA_NAME_dict        = dict(zip(population_df["LSOA_2011"], population_df["LA_NAME"]))
    TARIFF_dict = dict(zip(population_df["LSOA_2011"], population_df["TARIFF"]))
    TARIFF_PER_CAPITA_dict = dict(zip(population_df["LSOA_2011"], population_df["TARIFF_PER_CAPITA"]))
    PATIENTS_WITH_UCP_dict = dict(zip(population_df["LSOA_2011"], population_df["PATIENTS_WITH_UCP"]))
    
    

    # --- Quintile edges (compute once for the selected metric) ---
    if highlight_metric == "TOTAL_PATIENTS":
        edges = compute_quintile_edges(population_df["TOTAL_PATIENTS"])
    elif highlight_metric == "PATIENTS_WITH_DX":
        edges = compute_quintile_edges(population_df["PATIENTS_WITH_DX"])
    elif highlight_metric == "PATIENTS_WITH_DX_PERC":
        edges = compute_quintile_edges(population_df["PATIENTS_WITH_DX_PERC"])
    elif highlight_metric == "TARIFF":
        edges = compute_quintile_edges(population_df["TARIFF"])
    elif highlight_metric == "TARIFF_PER_CAPITA":
        edges = compute_quintile_edges(population_df["TARIFF_PER_CAPITA"])
    elif highlight_metric == "PATIENTS_WITH_UCP":
        edges = compute_quintile_edges(population_df["PATIENTS_WITH_UCP"])
    elif highlight_metric == "IMD_DECILE":
        # Keep IMD on interpretable fixed bands; if you want quantiles here too,
        # replace this line with compute_quintile_edges(population_df["IMD_DECILE"])
        edges = np.array([1.0, 3.0, 5.0, 7.0, 9.0, 10.0], dtype=float)  # 1–2, 3–4, 5–6, 7–8, 9–10
    else:
        # safe default
        edges = compute_quintile_edges(population_df["TOTAL_PATIENTS"])


    # --- Enrich features + color ---
    for feature in geojson_data["features"]:
        props = feature["properties"]
        lsoa_code = props.get("LSOA11CD")
    
        TOTAL_PATIENTS        = int(TOTAL_PATIENTS_dict.get(lsoa_code, 0))
        PATIENTS_WITH_DX      = int(PATIENTS_WITH_DX_dict.get(lsoa_code, 0))
        PATIENTS_WITH_DX_PERC = float(PATIENTS_WITH_DX_PERC_dict.get(lsoa_code, 0.0))
        TARIFF                = float(TARIFF_dict.get(lsoa_code, 0.0))
        TARIFF_PER_CAPITA     = float(TARIFF_PER_CAPITA_dict.get(lsoa_code, 0.0))
        IMD_DECILE            = int(IMD_DECILE_dict.get(lsoa_code, 0))
        LA_NAME               = LA_NAME_dict.get(lsoa_code, "Unknown")
        PATIENTS_WITH_UCP     = int(PATIENTS_WITH_UCP_dict.get(lsoa_code, 0))
        
    
        # Attach properties used in tooltip/accessors
        props["TOTAL_PATIENTS"]               = TOTAL_PATIENTS
        props["PATIENTS_WITH_DX"]                = PATIENTS_WITH_DX
        props["PATIENTS_WITH_DX_PERC"]  = PATIENTS_WITH_DX_PERC
        props["PATIENTS_WITH_UCP"]      = fmt_int(PATIENTS_WITH_UCP)
        props["TARIFF"]                    = TARIFF
        props["TARIFF_PER_CAPITA"]         = TARIFF_PER_CAPITA
        props["IMD_DECILE"]               = IMD_DECILE
        props["LA_NAME"]                  = LA_NAME

        # Add preformatted strings for the tooltip
        props["TOTAL_PATIENTS_FMT"]          = fmt_int(TOTAL_PATIENTS)
        props["PATIENTS_WITH_DX_FMT"]        = fmt_int(PATIENTS_WITH_DX)
        props["PATIENTS_WITH_DX_PERC_FMT"]   = fmt_pct(PATIENTS_WITH_DX_PERC, dp=1)
        props["TARIFF_FMT"]                  = fmt_gbp(TARIFF)
        props["TARIFF_PER_CAPITA_FMT"]       = fmt_gbp(TARIFF_PER_CAPITA)
        
    
        # Select value to color by
        if highlight_metric == "TOTAL_PATIENTS":
            value = float(TOTAL_PATIENTS)
        elif highlight_metric == "PATIENTS_WITH_DX":
            value = float(PATIENTS_WITH_DX)
        elif highlight_metric == "PATIENTS_WITH_DX_PERC":
            value = float(PATIENTS_WITH_DX_PERC)
        elif highlight_metric == "PATIENTS_WITH_UCP":
            value = float(PATIENTS_WITH_UCP)
        elif highlight_metric == "TARIFF":
            value = float(TARIFF)
        elif highlight_metric == "TARIFF_PER_CAPITA":
            value = float(TARIFF_PER_CAPITA)
        elif highlight_metric == "IMD_DECILE":
            value = float(IMD_DECILE)
        else:  # fallback
            value = float(TOTAL_PATIENTS)
    
        # Color from quintiles
        props["fill_color"] = color_from_quintiles(value, edges, highlight_metric)
        props["line_color"] = [255, 255, 255, 255]


# (return geojson_data)  # <- do this AFTER the loop, in your function body

    return geojson_data

population_df = load_population_data()

# In Streamlit
# st.dataframe(population_df)

# st.subheader("Population Data Overview")
# col1, col2 = st.columns(2)

# with col1:
#     st.metric("Total LSOAs", len(population_df))                        # the number of LSOAs in the dataset
#     st.metric("Total Population", f"{population_df['TOTAL_PATIENTS'].sum():,}")  # COUNT is the population of each LSOA so sum() COUNT is Total Population

# with col2:
#     st.metric("Most Populous LSOA", population_df.iloc[0]["LSOA_2011"]) # because we sorted on TOTAL_PATIENTS Desc this is the name of the most populous LSOA
#     st.metric("Max Population", f"{population_df.iloc[0]['TOTAL_PATIENTS']:,}")  # because we sorted on TOTAL_PATIENTS Desc this is the COUNT of the most populous LSOA 

with st.expander(label="LSOA Metrics"):
    st.dataframe(population_df.head(20), use_container_width=True)  # click on LSOA metrics and you get a view of the dataset

# st.subheader("Interactive Population Density Map")

highlight_option = st.selectbox(
    "Choose what to highlight on the map:",
    ("Population", "Index of Multiple Deprivation Decile", "Patients with LTC", "People with any LTC %", "Tariff (£)","Tariff per capita (£)","Patients with Urgent Care Plan"),
    key="highlight_metric_select",
)

option_to_metric = {
    "Population": "TOTAL_PATIENTS",
    "Index of Multiple Deprivation Decile": "IMD_DECILE",
    "Patients with LTC": "PATIENTS_WITH_DX",
    "People with any LTC %": "PATIENTS_WITH_DX_PERC",
    "Patients with Urgent Care Plan": "PATIENTS_WITH_UCP", 
    "Tariff (£)": "TARIFF",
    "Tariff per capita (£)": "TARIFF_PER_CAPITA"
}

highlight_metric = option_to_metric.get(highlight_option, "TOTAL_PATIENTS")


# Create GeoJSON with selected highlighting
geojson_with_pop = create_geojson_with_population(highlight_metric)

# Create pydeck chart with GeoJson layer
geojson_layer = pdk.Layer(
    "GeoJsonLayer",
    geojson_with_pop,
    pickable=True,
    stroked=True,
    filled=True,
    extruded=False,
    get_fill_color="properties.fill_color",
    get_line_color="properties.line_color",
    get_line_width=5,
    opacity=0.8,
)

# Set the viewport location
view_state = pdk.ViewState(latitude=51.4045, longitude=-0.0245, zoom=10, pitch=0)


deck = pdk.Deck(
    layers=[geojson_layer],
    initial_view_state=view_state,
    tooltip={
        "html": (
            "<b>{LSOA11NM}</b><br/>"
            "LSOA Code: {LSOA11CD}<br/>"
            "LSOA Population: {TOTAL_PATIENTS_FMT}<br/>"
            "People with LTC: {PATIENTS_WITH_DX_FMT}<br/>"
            "People with any LTC %: {PATIENTS_WITH_DX_PERC_FMT}<br/>"
            "Patients with Urgent Care Plan: {PATIENTS_WITH_UCP}<br/>"
            "Tariff: {TARIFF_FMT}<br/>"
            "Tariff per capita: {TARIFF_PER_CAPITA_FMT}<br/>"
            "Deprivation Decile: {IMD_DECILE}<br/>"
            "Local Authority: {LA_NAME}"
        ),
        "style": {"backgroundColor": "steelblue", "color": "white"},
    },
    map_style=pdk.map_styles.CARTO_LIGHT,
)


left, right = st.columns([3, 1])  # tweak ratios to taste

with left:
    st.pydeck_chart(deck, use_container_width=True, height=800)

def edges_for_metric(df: pd.DataFrame, metric: str) -> np.ndarray:
    if metric == "TOTAL_PATIENTS":
        return compute_quintile_edges(df["TOTAL_PATIENTS"])
    if metric == "PATIENTS_WITH_DX":
        return compute_quintile_edges(df["PATIENTS_WITH_DX"])
    if metric == "PATIENTS_WITH_DX_PERC":
        return compute_quintile_edges(df["PATIENTS_WITH_DX_PERC"])
    if metric == "PATIENTS_WITH_UCP":
        return compute_quintile_edges(df["PATIENTS_WITH_UCP"])
    if metric == "TARIFF":
        return compute_quintile_edges(df["TARIFF"])
    if metric == "TARIFF_PER_CAPITA":
        return compute_quintile_edges(df["TARIFF_PER_CAPITA"])
    if metric == "IMD_DECILE":
        return np.array([1.0, 3.0, 5.0, 7.0, 9.0, 10.0], dtype=float)

edges_ui = edges_for_metric(population_df, highlight_metric)

with right:
    st.subheader(f"{highlight_option} (quintiles)")

    if highlight_metric == "IMD_DECILE":
        labels = ["1–2", "3–4", "5–6", "7–8", "9–10"]
        colors = PALETTE_IMD

    elif highlight_metric == "PATIENTS_WITH_DX_PERC":
        labels = [f"{edges_ui[i]:.1f}–{edges_ui[i+1]:.1f}%" for i in range(5)]
        colors = PALETTE_COUNTS
    elif highlight_metric == "TARIFF":
        labels = [f"{fmt_gbp(edges_ui[i])}–{fmt_gbp(edges_ui[i+1])}" for i in range(5)]
        colors = PALETTE_COUNTS
    elif highlight_metric == "TARIFF_PER_CAPITA":
        labels = [f"{fmt_gbp(edges_ui[i])}–{fmt_gbp(edges_ui[i+1])}" for i in range(5)]
        colors = PALETTE_COUNTS
    else:
        labels = [f"{int(round(edges_ui[i])):,}–{int(round(edges_ui[i+1])):,}" for i in range(5)]
        colors = PALETTE_COUNTS

    for lab, (r, g, b) in zip(labels, colors):
        st.markdown(
            f"<div style='display:flex;align-items:center;margin-bottom:6px'>"
            f"<span style='display:inline-block;width:16px;height:16px;"
            f"background:rgb({r},{g},{b});border-radius:3px;margin-right:8px'></span>"
            f"{lab}</div>",
            unsafe_allow_html=True,
        )



        

# # Display the deck
# st.pydeck_chart(deck, height=800)



# # # Add summary statistics for the map
# # st.subheader("Map Data Summary")
# # col1, col2, col3 = st.columns(3)

# # with col1:
# #     st.metric("Total LSOAs", len(geojson_with_pop["features"]))

# # with col2:
# #     st.metric("Average Population", f"{population_df['TOTAL_PATIENTS'].mean():.0f}")

# # with col3:
# #     st.metric(
# #         "Population Range",
# #         f"{population_df['TOTAL_PATIENTS'].min():,} - {population_df['TOTAL_PATIENTS'].max():,}",
# #     )
    
# # # Add color legend based on selected metric
# st.subheader("Color Legend")
# if highlight_metric == "population":
#     st.markdown("""
#     - **Blue areas**: Lower population density
#     - **Red areas**: Higher population density  
#     - **Gray areas**: No population data available
#     """)
# elif highlight_metric == "has_dx_perc":
#     st.markdown("""
#     - **Blue areas**: Lower % of patients with any LTCs
#     - **Red areas**: Higher % of patients with any LTCs  
#     """)
# else:  # IMD decile
#     st.markdown("""
#     - **Red areas**: Most deprived (IMD decile 1-3)
#     - **Orange/Yellow areas**: Moderately deprived (IMD decile 4-6)
#     - **Green areas**: Least deprived (IMD decile 7-10)
#     - **Gray areas**: No deprivation data available
#     """)

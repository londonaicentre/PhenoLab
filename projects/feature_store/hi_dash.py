import streamlit as st
import pandas as pd
from scipy import stats
import numpy as np
import altair as alt
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
import json
import folium
from streamlit_folium import folium_static
import geopandas as gpd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# This generates a tabbed streamlit dashboard with:
# 1) Descriptive prevalence
# 2) Adjusted risk + age of onset
# 3) Geospatial predicted risk
# 4) Synthetic patient panel
# POC for now - needs heavy, heavy refacctoring!!

# Order of demographic variables to display
DEMOGRAPHIC_ORDER = {
    'ethnicity': ['White', 'South Asian', 'Black', 'East Or Other Asian'],
    'london_imd': ['1', '2', '3', '4', '5'],
    'age_band': ['0-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75+'],
    'gender': ['Male', 'Female']
}

# Ethnicity colour dict
ETHNICITY_COLOURS = {
    'White': '#00b4d8',      # cyan-blue
    'South Asian': '#ff7b00',   # orange
    'Black': '#00e676',      # bright green
    'East Or Other Asian': '#ff4d6d'  # coral red
}

# Gender colour dict
GENDER_COLOURS = {
    'Male': '#2e8b57', # sea green
    'Female': '#98fb98' # pale green
}

# Helper functions
def load_phenotypes():
    """
    Loads phenotype configuration from JSON file
    Returns the phenotype keys (short names) used consistently across the feature store
    """
    with open("phenoconfig.json", "r") as f:
        pheno_dict = json.load(f)
        print(list(pheno_dict.keys()))
    return list(pheno_dict.keys())

def convert_decimal_columns(df):
    """
    Convert all numeric-like columns to float for JSON serialization
    """
    for column in df.columns:
        try:
            df[column] = pd.to_numeric(df[column])
        except (ValueError, TypeError):
            continue
    return df

# Descriptive prevalence charts
def get_prevalence(snowsesh, phenotype, demographic):
    """
    Gets prevalence data for a specific phenotype and selected demographic
    Used in simple prevalence chart
    """
    query = f"""
    SELECT
        DEMOGRAPHIC_VALUE as DEMOGRAPHIC_SUBGROUP,
        DEMOGRAPHIC_DENOMINATOR as SUBGROUP_POPULATION,
        CAST("{phenotype}_PREVALENCE" as FLOAT) as PREVALENCE,
        CAST("{phenotype}_COUNT" as FLOAT) as COUNT,
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PHENOTYPE_DEMOGRAPHIC_PREVALENCE
    WHERE DEMOGRAPHIC_TYPE = '{demographic}'
    ORDER BY PREVALENCE DESC
    """
    df = snowsesh.execute_query_to_df(query)

    df['PREVALENCE'] = pd.to_numeric(df['PREVALENCE'], errors='coerce')
    df['COUNT'] = pd.to_numeric(df['COUNT'], errors='coerce')
    df['SUBGROUP_POPULATION'] = pd.to_numeric(df['SUBGROUP_POPULATION'], errors='coerce')

    if demographic == 'gender':
        df = df[df['DEMOGRAPHIC_SUBGROUP'].isin(['Male', 'Female'])]
    if demographic == 'ethnicity':
        df = df[df['DEMOGRAPHIC_SUBGROUP'].isin(['White', 'Black', 'South Asian', 'East Or Other Asian'])]

    if demographic in DEMOGRAPHIC_ORDER:
        order = DEMOGRAPHIC_ORDER[demographic]
        df = df[df['DEMOGRAPHIC_SUBGROUP'].isin(order)]
        df['DEMOGRAPHIC_SUBGROUP'] = pd.Categorical(df['DEMOGRAPHIC_SUBGROUP'], categories=order, ordered=True)
        df = df.sort_values('DEMOGRAPHIC_SUBGROUP')

    return df

def create_simple_prevalence_chart(snowsesh, phenotype, demographic, title):
    """
    Creates a prevalence chart for a given phenotype and demographic
    """
    df = get_prevalence(snowsesh, phenotype, demographic)

    # Format prevalence for display
    df['PREVALENCE_FORMATTED'] = df['PREVALENCE'].round(1).astype(str) + '%'

    if demographic == 'ethnicity':
        color_encoding = alt.Color('DEMOGRAPHIC_SUBGROUP:N',
                                 scale=alt.Scale(domain=list(ETHNICITY_COLOURS.keys()),
                                               range=list(ETHNICITY_COLOURS.values())),
                                 legend=None)
    elif demographic == 'gender':
        color_encoding = alt.Color('DEMOGRAPHIC_SUBGROUP:N',
                                 scale=alt.Scale(domain=list(GENDER_COLOURS.keys()),
                                               range=list(GENDER_COLOURS.values())),
                                 legend=None)
    else:
        color_encoding = alt.Color('PREVALENCE:Q',
                                 scale=alt.Scale(scheme='yellowgreenblue',
                                               domain=[0, df['PREVALENCE'].max()],
                                               type='linear'),
                                 legend=None)

    # Create Altair chart from df
    base = alt.Chart(df).encode(
        y=alt.Y('DEMOGRAPHIC_SUBGROUP:N',
                title=None,
                sort=None,
                axis=alt.Axis(orient='right')),  # Use pre-sorted order
        x=alt.X('PREVALENCE:Q',
                title='Prevalence Within Subgroup (%)',
                scale=alt.Scale(domain=[0, df['PREVALENCE'].max() * 1.2])),  # Add 20% padding
        color=color_encoding,
        tooltip=[
            alt.Tooltip('DEMOGRAPHIC_SUBGROUP', title='Group'),
            alt.Tooltip('PREVALENCE:Q', title='Prevalence Within Subgroup', format='.1f')
        ]
    )

    # Create layered bars and text
    chart = alt.layer(
        base.mark_bar(opacity=0.7),
        base.encode(
            text=alt.Text('PREVALENCE_FORMATTED:N')
        ).mark_text(
            align='left',
            dx=5,
            color='black'
        )
    ).properties(
        title=title,
        width=400,
        height=420
    ).configure_axis(
        grid=True
    ).configure_view(
        strokeWidth=0
    )

    st.altair_chart(chart, use_container_width=False)

# Age of Onset chart (faceted area chart)
def get_onset_age_data(snowsesh, phenotype):
    """
    Gets age of onset data for a specific phenotype
    """
    query = f"""
    SELECT GENDER, AGE_AT_ONSET, ETHNIC_AIC_CATEGORY, IMD_QUINTILE
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_PHENOTYPE_AGE_OF_ONSET
    WHERE PHENOTYPE_NAME = '{phenotype}'
    """
    df = snowsesh.execute_query_to_df(query)

    df = df[df['GENDER'].isin(['Male', 'Female'])]

    ethnicity_order = ['White', 'South Asian', 'Black', 'East Or Other Asian']
    df = df[df['ETHNIC_AIC_CATEGORY'].isin(ethnicity_order)]
    df['ETHNIC_AIC_CATEGORY'] = pd.Categorical(
        df['ETHNIC_AIC_CATEGORY'],
        categories=ethnicity_order,
        ordered=True
    )

    df['AGE_AT_ONSET'] = pd.to_numeric(df['AGE_AT_ONSET'], errors='coerce')
    df = df[df['AGE_AT_ONSET'] > 0]
    df = df[df['AGE_AT_ONSET'] < 100]
    df = df.dropna(subset=['AGE_AT_ONSET'])

    return df

def create_faceted_onset_chart(snowsesh, phenotype, demographic_col, title):
    """
    Creates a simple faceted area chart showing age of onset distribution
    """
    df = get_onset_age_data(snowsesh, phenotype)
    n_categories = df[demographic_col].nunique()
    facet_height = (380-(10*(n_categories-1))) / (n_categories + 1)

    # directly specify order here
    if demographic_col == 'ETHNIC_AIC_CATEGORY':
        sort_order = ['White', 'South Asian', 'Black', 'East Or Other Asian']
    else:
        sort_order = None

    chart = alt.Chart(df).transform_density(
        'AGE_AT_ONSET',
        as_=['AGE_AT_ONSET', 'density'],
        groupby=[demographic_col]
    ).mark_area(
        opacity=0.7
    ).encode(
        x=alt.X('AGE_AT_ONSET:Q',
                title='Age at Onset',
                scale=alt.Scale(domain=[0, 100], padding=0)),
        y=alt.Y('density:Q', title=None),
        color=alt.condition(
            alt.datum[demographic_col],
            alt.Color(f'{demographic_col}:N',
                     scale=alt.Scale(domain=list(ETHNICITY_COLOURS.keys()),
                                   range=list(ETHNICITY_COLOURS.values()))
                     if demographic_col == 'ETHNIC_AIC_CATEGORY' else
                     alt.Scale(scheme='lightorange')),
            alt.value('steelblue')
        ),
        row=alt.Row(f'{demographic_col}:N', sort=sort_order),
    ).properties(
        height=facet_height,
        width=600,
        title=title
    ).configure_facet(
        spacing=10  # this is spacing between facets
    ).resolve_scale(
        y='independent'  # each facet gets own y-scale
    )
    st.altair_chart(chart, use_container_width=False)

# Effects modification chart for risk of phenotype
def get_adjusted_effects(snowsesh, phenotype):
    """
    Gets adjusted effects data for a specific phenotype from Snowflake
    """
    # # Map from DB name to short name using reverse mapping
    # phenotype_short = {v: k for k, v in PHENOTYPE_MAPPER.items()}[phenotype_db_name]

    query = f"""
    SELECT *
    FROM INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PHENOTYPE_ADJUSTED_EFFECTS
    WHERE "phenotype" = '{phenotype}'
    """
    df = snowsesh.execute_query_to_df(query)
    return df

def create_effect_modification_chart(df, effect_type, title, show_legend=True):
    """
    Creates chart showing effect modification with confidence intervals
    """
    # Filter data for specific effect type (i.e. age or deprivation)
    plot_df = df[df['effect_modifier'] == effect_type].copy()

    if effect_type == 'age_band':
        # Custom age band axis with specified order
        x_encoding = alt.X(
            'modifier_value:N',
            title='Age Band',
            sort=['0-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65-74', '75+'],
            axis=alt.Axis(
                labelAngle=-45,
                labelFontSize=12,
                titleFontSize=14
            )
        )
    else:
        # Continuous axis for IMD with custom marks
        x_encoding = alt.X(
            'modifier_value:Q',
            title='IMD Rank (0=Most Deprived, 1=Least Deprived)',
            axis=alt.Axis(
                values=[0, 0.2, 0.4, 0.6, 0.8, 1.0],
                labelFontSize=12,
                titleFontSize=14
            )
        )

    ETHNICITY_COLOURS = {
        'White': '#00b4d8',      # cyan/blue
        'South_Asian': '#ff7b00', # orange
        'Black': '#00e676',      # bright green
        'East_Or_Other_Asian': '#ff4d6d'  # coral red
    }

    # base chart
    base = alt.Chart(plot_df).encode(
        x=x_encoding,
        y=alt.Y(
            'odds_ratio:Q',
            title='Odds Ratio',
            scale=alt.Scale(type='linear'),
            axis=alt.Axis(
                labelFontSize=12,
                titleFontSize=14
            )
        ),
        color=alt.Color(
            'ethnic_group:N',
            scale=alt.Scale(
                domain=list(ETHNICITY_COLOURS.keys()),
                range=list(ETHNICITY_COLOURS.values())
            ),
            legend=alt.Legend(
                orient='right',
                title='Ethnic Group',
                titleFontSize=12,
                labelFontSize=12
            ) if show_legend else None
        )
    )
    # confidence interval band
    ci_bands = base.mark_area(opacity=0.1).encode(
        y='lower_ci:Q',
        y2='upper_ci:Q'
    )
    # Line for main effect
    lines = base.mark_line(size=2)
    # combine above layers
    chart = (ci_bands + lines).properties(
        title=alt.Title(
            text=title,
            fontSize=14,
            anchor='middle'
        ),
        width=800,
        height=500
    ).configure_axis(
        grid=True,
        gridOpacity=0.2
    )
    return chart

# Predicted geospatial risk
def get_risk_data(snowsesh):
    """
    Retrieves predicted risk data and IMD ranks by LSOA
    """
    load_dotenv()

    try:
        query = """
        WITH LSOA_IMD AS (
            SELECT
                PATIENT_LSOA_2011,
                AVG(LONDON_IMD_RANK) as AVG_IMD_RANK
            FROM PERSON_NEL_MASTER_INDEX
            WHERE PATIENT_STATUS = 'ACTIVE'
            AND INCLUDE_IN_LIST_SIZE_FLAG = 1
            GROUP BY PATIENT_LSOA_2011
        )
        SELECT
            r.*,
            i.AVG_IMD_RANK
        FROM PHENOTYPE_GEOSPATIAL_RISK r
        LEFT JOIN LSOA_IMD i ON r.PATIENT_LSOA_2011 = i.PATIENT_LSOA_2011
        """

        df = snowsesh.execute_query_to_df(query)
        df = convert_decimal_columns(df)
        return df

    except Exception as e:
        st.error(f"Error retrieving data: {e}")
        raise e

def create_choropleth_map(gdf, value_column, title, color_scheme='YlOrRd', reverse=False):
    """
    Creates a Folium choropleth map for the specified metric
    """
    # Create base map
    m = folium.Map(
        location=[gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()],
        zoom_start=12.9,
        tiles='CartoDB dark_matter' # dark mode
    )

    # Create choropleth layer
    folium.Choropleth(
        geo_data=gdf.__geo_interface__,
        name='choropleth',
        data=gdf,
        columns=['LSOA11CD', value_column],
        key_on='feature.properties.LSOA11CD',
        fill_color=color_scheme,
        fill_opacity=0.6,
        line_opacity=0.5,
        legend_name=title,
        reverse=reverse
    ).add_to(m)

    #Add hover tooltips
    tooltip_fields = ['LSOA11NM', value_column]
    tooltip_aliases = ['LSOA Name:', f'{title}:']

    #Add population and case counts for context
    if value_column not in ['population', 'actual_cases']:
        tooltip_fields.extend(['population', 'actual_cases'])
        tooltip_aliases.extend(['Population:', 'Actual Cases:'])

    NIL = folium.features.GeoJson(
        gdf,
        style_function=lambda x: {
            'fillColor': '#ffffff',
            'color': '#000000',
            'fillOpacity': 0.1,
            'weight': 0.1
        },
        highlight_function=lambda x: {
            'fillColor': '#000000',
            'color': '#000000',
            'fillOpacity': 0.50,
            'weight': 0.1
        },
        tooltip=folium.features.GeoJsonTooltip(
            fields=tooltip_fields,
            aliases=tooltip_aliases,
            style="background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;"
        )
    )

    m.add_child(NIL)
    m.keep_in_front(NIL)

    return m

@st.cache_data
def load_segment_data():
    df_3d = pd.read_csv('clustering_segment_risk/ppmi_3d.csv')
    df_2d = pd.read_csv('clustering_segment_risk/ppmi_2d.csv', index_col=0)
    df_3d_1834 = pd.read_csv('clustering_segment_risk/ppmi_3d_age18-34.csv')
    df_2d_1834 = pd.read_csv('clustering_segment_risk/ppmi_2d_age18-34.csv', index_col=0)
    df_3d_3564 = pd.read_csv('clustering_segment_risk/ppmi_3d_age35-64.csv')
    df_2d_3564 = pd.read_csv('clustering_segment_risk/ppmi_2d_age35-64.csv', index_col=0)
    df_3d_6584 = pd.read_csv('clustering_segment_risk/ppmi_3d_age65-84.csv')
    df_2d_6584 = pd.read_csv('clustering_segment_risk/ppmi_2d_age65-84.csv', index_col=0)
    #max_ppmi_3d = df_3d['ppmi'].max()
    return df_3d, df_2d, df_3d_1834, df_2d_1834, df_3d_3564, df_2d_3564, df_3d_6584, df_2d_6584

def show_segdash(selected_condition):
    # Load the data
    df_3d, df_2d, df_3d_1834, df_2d_1834, df_3d_3564, df_2d_3564, df_3d_6584, df_2d_6584 = load_segment_data()

    age_group = st.radio(
        "Age Group",
        ["All", "18-34", "35-84"],
        horizontal=True,
        label_visibility="collapsed"
    )

    if age_group == "All":
        data_3d = df_3d
        data_2d = df_2d
    elif age_group == "18-34":
        data_3d = df_3d_1834
        data_2d = df_2d_1834
    elif age_group == "35-84":
        data_3d = df_3d_3564
        data_2d = df_2d_3564


    conditions = sorted(data_2d.columns)

    # Prepare 2D data for visualization
    two_d_data = data_2d[selected_condition].reset_index()
    two_d_data.columns = ['condition', 'ppmi']
    two_d_data = two_d_data[two_d_data['condition'] != selected_condition]  # Remove self-comparison
    two_d_data = two_d_data.sort_values('ppmi', ascending=False)
    two_d_data = two_d_data.head(10)

    # Create 2D bar chart
    st.subheader(f'Dual Condition Multi-Morbidity with {selected_condition}')
    bar_chart = alt.Chart(two_d_data).mark_bar().encode(
        y=alt.Y('condition:N',
                title='Condition',
                sort='-x',
                axis=alt.Axis(labelLimit=200)),  # Sort by PPMI value
        x=alt.X('ppmi:Q',
                title='PPMI Score'),
        color=alt.Color('ppmi:Q',
                    scale=alt.Scale(scheme='darkmulti', domain=[0, 1.5]),
                    title='PPMI Score'),
        tooltip=[
            alt.Tooltip('condition:N', title='Condition'),
            alt.Tooltip('ppmi:Q', title='PPMI', format='.3f')
        ]
    ).properties(
        width=800,
        height=400
    )

    st.altair_chart(bar_chart, use_container_width=True)

    # 3D data filtering
    filtered_data = data_3d[
        (data_3d['condition3'] == selected_condition) &
        (data_3d['condition1'] != data_3d['condition2']) &
        (data_3d['condition1'] != selected_condition) &
        (data_3d['condition2'] != selected_condition) &
        (data_3d['condition1'] < data_3d['condition2'])
    ].copy()

    conditions_subset = [c for c in conditions if c != selected_condition]

    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader(f'Three Condition Multi-Morbidity with {selected_condition}')
        heatmap = alt.Chart(filtered_data).mark_rect().encode(
            x=alt.X('condition1:N',
                    title='Condition 1',
                    sort=conditions_subset),
            y=alt.Y('condition2:N',
                    title='Condition 2',
                    sort=conditions_subset),
            color=alt.Color('ppmi:Q',
                        scale=alt.Scale(scheme='darkmulti', domain=[0, 5]),
                        title='PPMI Score'),
            tooltip=[
                alt.Tooltip('condition1:N', title='Condition 1'),
                alt.Tooltip('condition2:N', title='Condition 2'),
                alt.Tooltip('ppmi:Q', title='PPMI', format='.3f'),
                alt.Tooltip('count:Q', title='Count', format=',')
            ]
        ).properties(
            width=600,
            height=600
        )
        st.altair_chart(heatmap, use_container_width=True)

    with col2:
        st.subheader('Top Multi-Morbidity Phenotypes')
        top_relationships = filtered_data.nlargest(10, 'ppmi')
        top_relationships = top_relationships[['condition1', 'condition2', 'ppmi', 'count']]
        top_relationships = top_relationships.round({'ppmi': 3})
        st.dataframe(top_relationships, height=400)


def create_patient_timeline():
    df = pd.read_csv('prisk0.csv')

    df['DATE'] = pd.to_datetime(df['DATE'], format='%d/%m/%Y')

    encounters_df = pd.melt(
        df,
        id_vars=['DATE'],
        value_vars=['PRIMARY_CARE', 'ACUTE_CARE'],
        var_name='Care Type',
        value_name='Encounters'
    )

    # base chart for encounters (bars)
    bars = alt.Chart(encounters_df).mark_bar().encode(
        x=alt.X('DATE:T', title='Date'),
        xOffset='Care Type:N',  # This creates the side-by-side bars
        y=alt.Y('Encounters:Q', title='Number of Encounters'),
        color=alt.Color('Care Type:N',
                       scale=alt.Scale(
                           #domain=['PRIMARY_CARE', 'ACUTE_CARE'],
                           #range=['#87CEEB', '#FFD700']
                       ),
                       legend=alt.Legend(
                           title='Care Type',
                           orient='bottom',
                           direction='horizontal',
                           titleOrient='left'
                       )
        ),
        tooltip=[
            alt.Tooltip('DATE:T', title='Date'),
            alt.Tooltip('Care Type:N', title='Type'),
            alt.Tooltip('Encounters:Q', title='Count')
        ]
    )

    acute_tooltips = alt.Chart(df).mark_bar().encode(
        x='DATE:T',
        y='ACUTE_CARE:Q',
        tooltip=[
            alt.Tooltip('DATE:T', title='Date'),
            alt.Tooltip('ACUTE_CARE:Q', title='Admissions'),
            alt.Tooltip('ACUTE_CARE_FLAG:N', title='Reason')
        ],
        opacity=alt.value(0)
    )

    # Line chart for risk score
    line = alt.Chart(df).mark_line(
        color='#FF6B6B',
        strokeWidth=2
    ).encode(
        x='DATE:T',
        y=alt.Y('RISK_SCORE:Q',
                title='Risk Score',
                axis=alt.Axis(titleColor='#FF6B6B')),
        tooltip=[
            alt.Tooltip('DATE:T', title='Date'),
            alt.Tooltip('RISK_SCORE:Q', title='Risk Score', format='.2f')
        ]
    )

    # Combine charts
    combined = alt.layer(bars, acute_tooltips, line).resolve_scale(
        y='independent'
    ).properties(
        width=800,
        height=400
    ).configure_view(
        strokeWidth=0
    ).configure_axis(
        grid=True
    )

    return combined

def create_hypertension_sankey():
    # Define nodes and their positions
    nodes = dict(
        pad=15,
        thickness=20,
        line=dict(color="white", width=0.5),
        label=[
            "Hypertension",                      # 0 (left)
            "Simple Hypertension",               # 1 (middle-left)
            "Simple HTN, Monitored",             # 2 (right)
            "HTN, Poor Control",                 # 3 (right)
            "HTN, Needs Monitoring",             # 4 (right)
            "HTN w/ Renal dysfunction",          # 5 (right)
            "HTN w/ End-Organ Damage",           # 6 (right)
            "HTN w/ Severe Organ Dysfunction"    # 7 (right)
        ],
        x = [0.1, 0.4, 0.9, 0.9, 0.9, 0.9, 0.9, 0.9],
        y = [0.5, 0.3, 0.1, 0.3, 0.5, 0.7, 0.85, 0.95],
        color=[
            "rgba(255, 160, 122, 0.7)",  # Light salmon
            "rgba(255, 140, 97, 0.7)",   # Lighter orange
            "rgba(255, 120, 71, 0.7)",   # Light orange
            "rgba(255, 99, 46, 0.7)",    # Orange
            "rgba(255, 78, 20, 0.7)",    # Dark orange
            "rgba(230, 57, 0, 0.7)",     # Orange-red
            "rgba(204, 0, 0, 0.7)",      # Red
            "rgba(179, 0, 0, 0.7)"       # Dark red
        ]
    )

    # Define the flows with adjusted proportions
    links = dict(
        source=[0, 0, 0, 0, 1, 1, 1],
        target=[1, 5, 6, 7, 2, 3, 4],
        value=[70, 15, 10, 5, 45, 15, 10],
        color=[
            "rgba(255, 140, 97, 0.7)",   # Main flow to Simple HTN
            "rgba(230, 57, 0, 0.7)",     # Flow to Renal dysfunction
            "rgba(204, 0, 0, 0.7)",      # Flow to End-organ damage
            "rgba(179, 0, 0, 0.7)",      # Flow to Severe dysfunction
            "rgba(255, 120, 71, 0.7)",   # Flow to Monitored
            "rgba(255, 99, 46, 0.7)",    # Flow to Poor Control
            "rgba(255, 78, 20, 0.7)"     # Flow to Needs Monitoring
        ]
    )

    # Create the figure
    fig = go.Figure(data=[go.Sankey(
        node=nodes,
        link=links,
        arrangement="fixed"
    )])

    # Update layout for dark theme
    fig.update_layout(
        font_size=12,
        font_color="white",
        height=350,
        width=1000,
        plot_bgcolor='rgba(17, 17, 17, 1)',
        paper_bgcolor='rgba(17, 17, 17, 1)',
        margin=dict(l=20, r=20, t=20, b=20)
    )

    return fig

def create_hypertension_forest_plot():
    # Define the phenotypes and their hazard ratios with CIs
    phenotypes = [
        'Simple HTN, Monitored',
        'HTN, Poor Control',
        'HTN, Needs Monitoring',
        'HTN w/ Renal dysfunction',
        'HTN w/ End-Organ Damage',
        'HTN w/ Severe Organ Dysfunction'
    ]

    # Clinically plausible hazard ratios and CIs
    hazard_ratios = [1.2, 1.8, 1.6, 2.1, 3.4, 4.8]
    ci_lower = [0.7, 0.8, 1.0, 1.8, 2.8, 3.7]
    ci_upper = [1.3, 2.2, 1.9, 2.5, 4.2, 6.2]

    # Calculate error bars
    error_minus = np.array(hazard_ratios) - np.array(ci_lower)
    error_plus = np.array(ci_upper) - np.array(hazard_ratios)

    # Create figure
    fig = go.Figure()

    # Add vertical line at HR = 1
    fig.add_vline(x=1, line_width=1, line_dash="dash", line_color="white", opacity=0.5)

    # Add the forest plot points and error bars
    fig.add_trace(go.Scatter(
        x=hazard_ratios,
        y=phenotypes,
        mode='markers',
        marker=dict(
            color='#FFA500',
            size=10,
            symbol='square'
        ),
        error_x=dict(
            type='data',
            symmetric=False,
            array=error_plus,
            arrayminus=error_minus,
            color='#FFA500',
            thickness=2,
            width=10
        ),
        name='Hazard Ratio'
    ))

    # Add text annotations for HRs and CIs
    for i, (hr, lower, upper) in enumerate(zip(hazard_ratios, ci_lower, ci_upper)):
        fig.add_annotation(
            x=upper + 0.5,  # Position text to the right of error bars
            y=i,
            text=f'HR: {hr:.1f} ({lower:.1f}-{upper:.1f})',
            showarrow=False,
            font=dict(color='white', size=12),
            xanchor='left'
        )

    # Update layout
    fig.update_layout(
        title=dict(
            text='',
            font=dict(size=12, color='white'),
            x=0.5,
            y=0.95
        ),
        xaxis=dict(
            title='Hazard Ratio (95% CI)',
            showgrid=True,
            gridcolor='rgba(255, 255, 255, 0.1)',
            zeroline=False,
            range=[0, 7],  # Adjust range based on your max CI
            tickfont=dict(color='white'),
            titlefont=dict(color='white')
        ),
        yaxis=dict(
            title='Phenotype',
            showgrid=False,
            zeroline=False,
            tickfont=dict(color='white'),
            titlefont=dict(color='white')
        ),
        plot_bgcolor='rgba(17, 17, 17, 1)',
        paper_bgcolor='rgba(17, 17, 17, 1)',
        showlegend=False,
        width=900,
        height=400,
        margin=dict(l=20, r=200, t=50, b=50)  # Extra right margin for HR text
    )

    return fig

def create_disease_progression():
    import plotly.graph_objects as go

    # Define colors
    colors = {
        'high_risk': 'rgba(220, 53, 69, 0.8)',  # red
        'medium_risk': 'rgba(255, 145, 0, 0.8)',  # orange
        'standard': 'rgba(79, 79, 79, 0.8)',  # gray
        'background': 'rgba(17, 17, 17, 1)',  # dark background
        'text': 'white'
    }

    # Create figure with custom layout
    fig = go.Figure()

    # Set up the base layout
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        showlegend=False,
        height=600,
        font=dict(color=colors['text']),
        margin=dict(l=20, r=20, t=40, b=20)
    )

    # Define y-positions
    main_y = 0.3  # Move main flow up
    node_spacing = 0.25  # Space between nodes
    base_node_y = -0.3  # Starting y position for nodes

    # Define stages and their conditions
    stages = [
        {
            'name': 'Simple HTN',
            'x': 0,
            'conditions': ['Essential Hypertension']
        },
        {
            'name': 'Simple HTN, Poor Control',
            'x': 1,
            'conditions': ['Essential Hypertension', 'Hypertension, Poor Control']
        },
        {
            'name': 'HTN, Renal Dysfunction',
            'x': 2,
            'conditions': ['Essential Hypertension', 'Hypertension, Poor Control', 'CKD Stage 1']
        },
        {
            'name': 'HTN w/ End-Organ Damage',
            'x': 3,
            'conditions': ['Essential Hypertension', 'Hypertension, Poor Control', 'CKD Stage 3']
        }
    ]

    # Add main flow arrows between stages
    for i in range(len(stages)-1):
        fig.add_trace(go.Scatter(
            x=[i+0, i+1],
            y=[main_y, main_y],
            mode='lines',
            line=dict(color='gray', width=2),
            hoverinfo='none'
        ))

    # Add stages and their condition nodes
    for stage in stages:
        x_pos = stage['x']

        # Add main stage box
        fig.add_shape(
            type="rect",
            x0=x_pos-0.2, x1=x_pos+0.2,
            y0=main_y-0.1, y1=main_y+0.1,
            fillcolor=colors['standard'],
            line=dict(color="white", width=1),
        )
        fig.add_annotation(
            x=x_pos, y=main_y,
            text=stage['name'],
            showarrow=False,
            font=dict(color="white", size=10)
        )

        # Add condition nodes - all below main line
        for i, condition in enumerate(stage['conditions']):
            y_pos = base_node_y - (i * node_spacing)

            # Add node
            fig.add_shape(
                type="circle",
                x0=x_pos-0.15, x1=x_pos+0.15,
                y0=y_pos-0.1, y1=y_pos+0.1,
                fillcolor=colors['standard'],
                line=dict(color="white", width=1),
            )

            # Add condition label
            fig.add_annotation(
                x=x_pos, y=y_pos,
                text=condition,
                showarrow=False,
                font=dict(color="white", size=10)
            )

            # Add arrow from node to main stage box
            fig.add_trace(go.Scatter(
                x=[x_pos, x_pos],
                y=[y_pos+0.1, main_y-0.1],  # Adjust connection points
                mode='lines',
                line=dict(color='gray', width=1),
                hoverinfo='none'
            ))

    # Add risk boxes
    risks = [
        ('Progression {End Stage Renal Failure}: MEDIUM', colors['medium_risk']),
        ('Progression {TIA/Stroke}: MEDIUM', colors['medium_risk']),
        ('Progression {Heart Failure}: HIGH', colors['high_risk'])
    ]

    # Add arrows and risk boxes
    for i, (risk, color) in enumerate(risks):
        y_pos = 0.6 - (i * 0.3)  # Spread risk boxes vertically

        # Add arrow from last stage to risk box
        fig.add_trace(go.Scatter(
            x=[3.2, 3.7],
            y=[main_y, y_pos],
            mode='lines',
            line=dict(color='gray', width=2),
            hoverinfo='none'
        ))

        # Add risk box
        fig.add_shape(
            type="rect",
            x0=3.7, x1=4.3,
            y0=y_pos-0.1, y1=y_pos+0.1,
            fillcolor=color,
            line=dict(color="white", width=1),
        )
        fig.add_annotation(
            x=4.0, y=y_pos,
            text=risk,
            showarrow=False,
            font=dict(color="white", size=10)
        )

    # Update axes with adjusted ranges to accommodate all elements
    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        visible=False,
        range=[-0.5, 4.5]
    )
    fig.update_yaxes(
        showgrid=False,
        zeroline=False,
        visible=False,
        range=[-1.2, 0.8]  # Adjusted to fit all elements
    )

    return fig

def main():
    load_dotenv()

    PHENOTYPES = load_phenotypes()

    if 'snowsesh' not in st.session_state:
        st.session_state.snowsesh = SnowflakeConnection()
        st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
        st.session_state.snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

    snowsesh = st.session_state.snowsesh

    st.set_page_config(layout="wide", initial_sidebar_state="expanded")

    # Phenotype selector in the sidebar
    with st.sidebar:  # Add the selectbox to the sidebar
        st.title("PRISM - Personalised Risk, Intelligent Stratification, and Modelling")
        st.write("_Proof of Concept Only_")
        selected_view = st.radio("Select View", ["Demographic Breakdown",
                                                 "Ethnicity Inequality",
                                                 "Underdiagnosis Risk",
                                                 "Statistical Segmentation",
                                                 "Segments and Risk",
                                                 "Personalised Profile"])
        phenotype = st.selectbox(
            "Select Phenotype",
            options=PHENOTYPES,
            format_func=lambda x: x.replace(" simple reference set", "").replace(" diagnoses", "")
        )

    # Display simple demographic breakdown charts
    if selected_view == "Demographic Breakdown":
        st.title(f"Descriptive Demographic Breakdown")
        st.write("""
            This view shows the prevalence of selected conditions across different demographic groups in the NEL population.
            Prevalence is broken down by: ethnic groups (including age of onset), socioeconomic deprivation, age bands, and gender.
            Views are generated by live SQL query execution in a Snowflake Database containing data for 2.5 million individuals.
        """)

        st.text("")

        if phenotype:
            cols = st.columns([3,2], gap="small")
            with cols[0]:
               create_faceted_onset_chart(snowsesh, phenotype, 'ETHNIC_AIC_CATEGORY', f'Age of Onset by Ethnicity')
            with cols[1]:
               create_simple_prevalence_chart(snowsesh, phenotype, 'ethnicity', 'Prevalence - Ethnic Subgroup')

            st.text("")

            # cols = st.columns(2, gap="small")
            # with cols[0]:
            #     create_simple_prevalence_chart(snowsesh, phenotype, 'ethnicity', 'Prevalence - Ethnic Subgroup')
            # with cols[1]:
            #     create_simple_prevalence_chart(snowsesh, phenotype, 'london_imd', 'Prevalence - Deprivation Quintile')

            cols = st.columns(3, gap="large")
            with cols[0]:
                create_simple_prevalence_chart(snowsesh, phenotype, 'age_band', 'Prevalence - Age Group')
            with cols[1]:
                create_simple_prevalence_chart(snowsesh, phenotype, 'london_imd', 'Prevalence - Deprivation Quintile')
            with cols[2]:
                create_simple_prevalence_chart(snowsesh, phenotype, 'gender', 'Prevalence - Gender')

    elif selected_view == "Ethnicity Inequality":
        st.title(f"Multivariate Adjusted Risk Profile")
        st.write("""
            Visualise how risk factors for conditions vary across ethnic groups when adjusting for other variables.
            Interactive charts show how ethnicity effects are modified by deprivation level and by age.
            This helps uncover optimal points and populations for intervention, as risk profiles increase unequally.
        """)

        effects_df = get_adjusted_effects(snowsesh, phenotype)
        if phenotype:
            # First row: Effect modification plot
            cols = st.columns(1)
            with cols[0]:
                imd_chart = create_effect_modification_chart(
                    effects_df,
                    'imd_rank',
                    'Ethnicity Effect Modification by Deprivation',
                    show_legend=True
                )
                st.altair_chart(imd_chart, use_container_width=True)

            # Second row: Effect modification plot 2
            cols = st.columns([3,2], gap="small")
            with cols[0]:
                imd_chart = create_effect_modification_chart(
                    effects_df,
                    'age_band',
                    'Ethnicity Effect Modification by Age',
                    show_legend=False
                )
                st.altair_chart(imd_chart, use_container_width=True)
            with cols[1]:
               create_simple_prevalence_chart(snowsesh, phenotype, 'ethnicity', 'Prevalence - Ethnic Subgroup')

            # Second row: Ethnicity age and prevalence
            # cols = st.columns([3,2], gap="small")
            # with cols[0]:
            #    create_faceted_onset_chart(snowsesh, phenotype, 'ETHNIC_AIC_CATEGORY', f'Age of Onset by Ethnicity')
            # with cols[1]:
            #    create_simple_prevalence_chart(snowsesh, phenotype, 'ethnicity', 'Prevalence - Ethnic Subgroup')

            # Third row: Deprivation age and prevalence
            # cols = st.columns([3,2], gap="small")
            # with cols[0]:
            #     create_faceted_onset_chart(snowsesh, phenotype, 'IMD_QUINTILE', f'Age of Onset by Deprivation Decile')
            # with cols[1]:
            #     create_simple_prevalence_chart(snowsesh, phenotype, 'london_imd', 'Prevalence - Deprivation Quintile')

    # Display geospatial visualisations
    elif selected_view == "Underdiagnosis Risk":
        st.title(f"GeoSpatial Analysis: Under-Diagnosis")
        st.write("""
            Models are applied to predictive task, uncovering geographic variations in diagnosis rates compared to expected prevalence across North-East London.
            Highlights areas where at-risk populations are risk of underdiagnosis, based on population characteristics and standardized differences.
            Enables precise interventions such as targeted screening.
        """)

        try:
            df_risk = get_risk_data(snowsesh)

            df_risk = df_risk[df_risk['phenotype'] == phenotype]

            # Read and merge geojson (should keep this on Snowflake)
            gdf = gpd.read_file("geostore/uk_lsoa.geojson")
            gdf = gdf.merge(df_risk, left_on='LSOA11CD', right_on='PATIENT_LSOA_2011', how='inner')
            gdf = gdf.dropna(subset=['geometry', 'standardized_difference', 'significant_under_diagnosis'])

            st.write("### Standardized Difference (Negative values are fewer cases than expected)")
            m1 = create_choropleth_map(
                gdf,
                'standardized_difference',
                'Standardized Difference',
                color_scheme='RdYlBu',
                reverse=True
            )
            folium_static(m1, width=1000)
            # Add summary statistics below maps
            st.write("### Summary Statistics")
            cols = st.columns(3)
            with cols[0]:
                st.metric("Total Population", f"{int(gdf['population'].sum()):,}")
            with cols[1]:
                st.metric("Known Cases", f"{int(gdf['actual_cases'].sum()):,}")
            with cols[2]:
                expected_vs_actual = (gdf['actual_cases'].sum() / gdf['expected_cases'].sum() - 1) * 100
                expected_vs_actual = expected_vs_actual - 5
                st.metric("Estimated Case Difference", f"{expected_vs_actual:.1f}%")

        except Exception as e:
            st.error(f"Error creating maps: {e}")
            st.write("Please ensure you have the required geojson file and data access.")

    # Display statistical segmentation page
    elif selected_view == "Statistical Segmentation":
        st.title(f"Statistical Segmentation")
        st.write("""
            Analyse how different conditions cluster together in dual and triple condition combinations.
            Shows strength of associations between conditions using Positive Pointwide Mutual Information.
            Resulting segments are clinically meaningful and statistically relevant.
        """)
        cols = st.columns(1)
        with cols[0]:
            show_segdash(phenotype)

    # Display segments and risk page
    elif selected_view == "Segments and Risk":
        st.title(f"Segments and Risk")
        st.write("""
            Clinically significant segments may experience varying levels of risk and disease progression, from simple hypertension to severe complications.
            This visualisation highlights key transition points and mortality risks across distinct phenotypes, helping identify critical intervention opportunities.
        """)
        htn_fig = create_hypertension_sankey()
        st.plotly_chart(htn_fig)

        st.write("### Hazard Ratio for 5-year Mortality")
        htn_forest = create_hypertension_forest_plot()
        st.plotly_chart(htn_forest)

    # Display individual profiler
    elif selected_view == "Personalised Profile":
        # to customise button width
        st.markdown("""
            <style>
            div.stButton > button:first-child {
                width: 180px;  /* Set a fixed width */
                margin-right: 10px;
            }

            div.stButton > button:nth-child(2) {  /*second button*/
                width: 180px;
            }
            </style>""", unsafe_allow_html=True)

        st.title("Patient Profiler")
        st.write("""
            What is personalised healthcare? Moving insights from the level of populations, or diseases, or pathways, to the individual patient.
            Patient-centric risk profiles, and complex and actionable phenotypes, allow meaningful action on an individual level. Proof of concept only.
        """)
        st.write("### Personalised Risk Radar")
        st.write("Higher index indicates higher risk or less optimisation")

        patient_data = {
            "Name": "[redacted_name]",
            "Gender": "Male",
            "DOB": "[redacted_date]",
            "Patient ID": "SK10983766",
            "NHS Number": "[redacted_number]",
            "GP Practice": "[redacted_name]",
        }

        # Categories and tooltips for the radar chart
        categories = ['Optimisation', 'Sociodemographic', 'Monitoring', 'Risk Profile',
                    'Hospital Admission', 'Frailty', 'Cost']

        # Individual patient values (higher = more risk/less optimization)
        patient_values = [0.74, 0.54, 0.64, 0.82, 0.73, 0.42, 0.85]

        # Age-adjusted population values (all lower than patient)
        age_pop_values = [0.22, 0.45, 0.20, 0.10, 0.12, 0.28, 0.10]

        # Segment-adjusted population values (mostly lower, some similar)
        segment_values = [0.65, 0.51, 0.52, 0.65, 0.55, 0.28, 0.80]

        tooltips = [
            "Risk of low medication adherence based on statement/order mismatch; Past history of poor medication adherence; Blood pressure uncontrolled despite Step 3 anti-HTN therapy",
            "Sociodemographic profile high risk for segment progression",
            "Recent follow-up | Regular follow-ups | Renal function monitoring outstanding",
            "High risk: Heart Failure | Moderate risk: TIA/Stroke | Moderate risk: Progression, ESRF",
            "High one-year hospital admission risk | Rising one-year hospital admission risk",
            "Frailty Score: Low | eFI: Low",
            "Cost Index: High"
        ]

        # Create figure with multiple traces
        fig_radar = go.Figure()

        # Close the polygons by repeating first value
        age_pop_values_closed = age_pop_values + [age_pop_values[0]]
        segment_values_closed = segment_values + [segment_values[0]]
        patient_values_closed = patient_values + [patient_values[0]]
        categories_closed = categories + [categories[0]]

        # Add age-adjusted population trace (blue)
        fig_radar.add_trace(go.Scatterpolar(
            r=age_pop_values_closed,
            theta=categories_closed,
            fill='toself',
            name='Age-matched Population',
            line=dict(color='rgba(0, 150, 255, 0.8)'),
            fillcolor='rgba(0, 150, 255, 0.2)',
        ))

        # Add segment-adjusted population trace (green)
        fig_radar.add_trace(go.Scatterpolar(
            r=segment_values_closed,
            theta=categories_closed,
            fill='toself',
            name='Segment-matched Population',
            line=dict(color='rgba(0, 255, 150, 0.8)'),
            fillcolor='rgba(0, 255, 150, 0.2)',
        ))

        # Add patient trace (orange-red)
        fig_radar.add_trace(go.Scatterpolar(
            r=patient_values_closed,
            theta=categories_closed,
            fill='toself',
            name='Patient',
            line=dict(color='rgba(255, 69, 0, 0.8)'),
            fillcolor='rgba(255, 69, 0, 0.2)',
            hovertemplate="%{theta}<br>Score: %{r:.2f}<br>%{text}<extra></extra>",
            text=tooltips,
        ))

        # Update layout
        fig_radar.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 1],
                    tickfont=dict(color="white")
                ),
                bgcolor="rgba(17, 17, 17, 1)",
                angularaxis=dict(
                    tickfont=dict(color="white")
                )
            ),
            paper_bgcolor="rgba(17, 17, 17, 1)",
            font_color="white",
            showlegend=True,
            legend=dict(
                font=dict(color="white"),
                y=-0.2,
                x=0.5,
                xanchor="center",
                orientation="h"
            ),
            height=700,
            width=800,
            margin=dict(b=80)  # Add bottom margin for legend
        )

        left_col, right_col = st.columns([2, 1])
        with left_col:
            st.plotly_chart(fig_radar)
        with right_col:
            patient_info_markdown = ""
            for key, value in patient_data.items():
                patient_info_markdown += f"**{key}:** {value}  \n" # format patient info on new lines

            st.markdown(patient_info_markdown)

            st.markdown("<br>" * 3, unsafe_allow_html=True)

            ltc_expander = st.expander("Flagged / Actionable Phenotypes", expanded=True)
            with ltc_expander:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("High Risk: Heart Failure", key="ltc1", type="secondary"):
                        st.session_state.message = "In segment with high 5-year risk of progression to Heart Failure"
                with col2:
                    if st.button("Medium Risk: TIA/Stroke", key="ltc2", type="secondary"):
                        st.session_state.message = "In segment with medium 5-year risk of TIA and/or Stroke"
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Medium Risk: Progression, ESRF", key="ltc3", type="secondary"):
                        st.session_state.message = "In segment with medium 5-year risk of progression to End Stage Renal Failure"
                with col2:
                    if st.button("Hypertension, Poor Control", key="ltc4", type="secondary"):
                        st.session_state.message = "Treatment: Step 3 | Last BP record: >4M | Last SBP: 182 | Last DBP: 100"
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Renal Function Recheck", key="ltc5", type="secondary"):
                        st.session_state.message = "Last UE record: >8M | Last Cr: 132 | Last Ur: 5.2 | Last eGFR: 44"
                with col2:
                    if st.button("Medication Compliance Review", key="ltc6", type="secondary"):
                        st.session_state.message = "Medication Statement vs Order mismatch detected | Delta: 2"

                # message box
                if "message" in st.session_state:
                    st.info(st.session_state.message)

        progression_fig = create_disease_progression()
        st.plotly_chart(progression_fig, use_container_width=True)

        st.write("### Encounter History & Admission Risk")
        st.altair_chart(create_patient_timeline(), use_container_width=True)

if __name__ == "__main__":
    main()
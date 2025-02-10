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
    df_3d = pd.read_csv('ppmi_3d.csv')
    df_2d = pd.read_csv('ppmi_2d.csv', index_col=0)
    max_ppmi_3d = df_3d['ppmi'].max()
    return df_3d, df_2d, max_ppmi_3d

def show_segdash(selected_condition):
    # Load the data
    data_3d, data_2d, max_ppmi_3d = load_segment_data()

    # Get unique conditions
    conditions = sorted(data_2d.columns)

    # Prepare 2D data for visualization
    two_d_data = data_2d[selected_condition].reset_index()
    two_d_data.columns = ['condition', 'ppmi']
    two_d_data = two_d_data[two_d_data['condition'] != selected_condition]  # Remove self-comparison
    two_d_data = two_d_data.sort_values('ppmi', ascending=False)  # Sort for better visualization
    two_d_data = two_d_data.head(10)

    # Create 2D bar chart
    st.subheader(f'Pairwise Relationships with {selected_condition}')
    bar_chart = alt.Chart(two_d_data).mark_bar().encode(
        y=alt.Y('condition:N',
                title='Condition',
                sort='-x',
                axis=alt.Axis(labelLimit=200)),  # Sort by PPMI value
        x=alt.X('ppmi:Q',
                title='PPMI Score'),
        color=alt.Color('ppmi:Q',
                    scale=alt.Scale(scheme='viridis', domain=[0, 5]),
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
        st.subheader(f'Three-way PPMI Heatmap for {selected_condition}')
        heatmap = alt.Chart(filtered_data).mark_rect().encode(
            x=alt.X('condition1:N',
                    title='Condition 1',
                    sort=conditions_subset),
            y=alt.Y('condition2:N',
                    title='Condition 2',
                    sort=conditions_subset),
            color=alt.Color('ppmi:Q',
                        scale=alt.Scale(scheme='viridis', domain=[0, 5]),
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
        st.subheader('Top 10 Strongest Relationships')
        top_relationships = filtered_data.nlargest(10, 'ppmi')
        top_relationships = top_relationships[['condition1', 'condition2', 'ppmi', 'count']]
        top_relationships = top_relationships.round({'ppmi': 3})
        st.dataframe(top_relationships, height=400)

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
        st.title("PRISM - Personalised Risk, Insights, Stratification and Modelling")
        selected_view = st.radio("Select View", ["Demographic Breakdown",
                                                 "Ethnicity Inequality",
                                                 "Underdiagnosis Risk",
                                                 "Statistical Segmentation",
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
                st.metric("Estimated Case Difference", f"{expected_vs_actual:.1f}%")

        except Exception as e:
            st.error(f"Error creating maps: {e}")
            st.write("Please ensure you have the required geojson file and data access.")

    # Display statistical segmentation page
    elif selected_view == "Statistical Segmentation":
        st.title(f"Statistical Segmentation")
        cols = st.columns(1)
        with cols[0]:
            show_segdash(phenotype)

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

        patient_data = {
            "Name": "[redacted_name]",
            "Gender": "Male",
            "DOB": "[redacted_date]",
            "Patient ID": "SK10983766",
            "NHS Number": "[redacted_number]",
            "GP Practice": "[redacted_name]",
        }

        # radar chart options
        categories = ['Medication', 'Sociodemographic', 'Follow-up', 'Risk Profile', 'Hospital Admission', 'Frailty', 'Cost']
        values = [0.82, 0.72, 0.64, 0.90, 0.73, 0.30, 0.65]
        tooltips = [
            "Low medication adherence",
            "Significant sociodemographic risk factors",
            "Recent follow-up; Regular follow-ups; Tests outstanding",
            "High cardiovascular risk; Moderate mental health risk",
            "High risk of one-year hospital admission",
            "Frailty Index: Low",
            "Cost Index: Moderate"
        ]

        fig_radar = go.Figure(data=go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            line=dict(color="skyblue"),
            hovertemplate="%{theta}<br>Score: %{r}<br>%{text}",
            text=tooltips,
        ))

        fig_radar.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 1]),
                bgcolor="rgb(10, 10, 10)"
            ),
            paper_bgcolor="rgb(10, 10, 10)",
            font_color="white",
            showlegend=False,
            height=700,
            width=800
        )

        st.title("Patient Phenotype Profiler")
        st.write("### _Proof-of-Concept Only_")

        left_col, right_col = st.columns([2, 1])

        with left_col:
            st.plotly_chart(fig_radar)

        with right_col:
            patient_info_markdown = ""
            for key, value in patient_data.items():
                patient_info_markdown += f"**{key}:** {value}  \n" # format patient info on new lines

            st.markdown(patient_info_markdown)

            ltc_expander = st.expander("Active LTCs", expanded=True)
            with ltc_expander:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("T2 Diabetes", key="ltc1", type="secondary"):
                        st.session_state.message = "Year of diagnosis: 2021; Medications: Metformin"
                with col2:
                    if st.button("Hypertension", key="ltc2", type="secondary"):
                        st.session_state.message = "Year of diagnosis: 2021; Medications: Ramipril, Amlodipine."
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("CAD", key="ltc3", type="secondary"):
                        st.session_state.message = "Year of diagnosis: 2024; Medications: Aspirin, Clopidogrel"
                with col2:
                    if st.button("Obesity", key="ltc4", type="secondary"):
                        st.session_state.message = "Year of diagnosis: 2015; Medications: []"
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("High Cholesterol", key="ltc5", type="secondary"):
                        st.session_state.message = "Year of diagnosis: 2015; Medications: Atorvastatin"
                with col2:
                    if st.button("Depression", key="ltc6", type="secondary"):
                        st.session_state.message = "Year if diagnosis: 2024; Medications: []"

            phenotype_expander = st.expander("Actionable Phenotypes", expanded=True)
            with phenotype_expander:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Requires HBA1c", key="phenotype1", type="secondary"):
                        st.session_state.message = "No HBA1c performed in past year"
                with col2:
                    if st.button("Medication Adherence", key="phenotype2"):
                        st.session_state.message = "Flagged for non-adherence to: Ramipril; Amlodipine; Atorvastatin"
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Statin Low Dose", key="phenotype3", type="secondary"):
                        st.session_state.message = "Risk factors indicate higher dose of Statin, or require documentation of reasons for non-escalation"
                with col2:
                    if st.button("Requires CKD Screen", key="phenotype4"):
                        st.session_state.message = "High risk of undiagnosed renal dysfunction, flag for renal function function and urine test"

            risk_expander = st.expander("Adverse Event Profile", expanded=True)
            with risk_expander:
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("High CV Risk", key="risk1"):
                        st.session_state.message = "QRISK2 12.5%; AIC-3YR Very High; See Actionable Phenotypes"
                with col2:
                    if st.button("Admission Risk", key="risk2"):
                        st.session_state.message = "AIC-1YR Very High; See Actionable Phenotypes"

        # message box
        if "message" in st.session_state:
            st.info(st.session_state.message)

if __name__ == "__main__":
    main()
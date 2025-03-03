import folium
import geopandas as gpd
import streamlit as st
from dotenv import load_dotenv
from streamlit_folium import folium_static

from phmlondon.snow_utils import SnowflakeConnection

# EXAMPLE SCRIPT FOR CREATING A GEOVISUALISATION
# TOTAL POPULATOIN PER LSOA


def get_lsoa_populations():
    """
    Retrieves population counts by LSOA2011 for active patients in list
    Returns a pandas DataFrame with LSOA2011 and POPULATIONSIZE columns
    """
    load_dotenv()

    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        query = """
        SELECT
            PATIENT_LSOA_2011,
            LEAST(COUNT(DISTINCT PERSON_ID), 10000) as POPULATIONSIZE
        FROM PERSON_NEL_MASTER_INDEX
        WHERE PATIENT_STATUS = 'ACTIVE'
        AND INCLUDE_IN_LIST_SIZE_FLAG = 1
        GROUP BY PATIENT_LSOA_2011
        ORDER BY PATIENT_LSOA_2011
        """

        df = snowsesh.execute_query_to_df(query)

        return df

    except Exception as e:
        print(f"Error retrieving LSOA populations: {e}")
        raise e
    finally:
        snowsesh.session.close()


def main():
    st.title("LSOA Population Distribution")

    df_pop = get_lsoa_populations()

    gdf = gpd.read_file("uk_lsoa.geojson")
    gdf = gdf.merge(df_pop, left_on="LSOA11CD", right_on="PATIENT_LSOA_2011", how="inner")

    st.write("### Population Distribution by LSOA")
    st.write(f"Showing population distribution across {len(gdf)} LSOAs")

    m = folium.Map(
        location=[gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean()], zoom_start=11
    )
    folium.Choropleth(
        geo_data=gdf.__geo_interface__,
        name="choropleth",
        data=gdf,
        columns=["LSOA11CD", "POPULATIONSIZE"],
        key_on="feature.properties.LSOA11CD",
        fill_color="YlOrRd",
        fill_opacity=0.7,
        line_opacity=0.2,
        legend_name="Population Size",
    ).add_to(m)

    folium_static(m)

    st.write("### LSOA Details")
    st.dataframe(
        gdf[["LSOA11CD", "LSOA11NM", "POPULATIONSIZE"]].sort_values(
            "POPULATIONSIZE", ascending=False
        )
    )


if __name__ == "__main__":
    main()

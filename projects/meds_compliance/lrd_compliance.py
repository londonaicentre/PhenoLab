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

# This code when ran with streamlit run, will open a dashboard that looks at lipid reglating drug prescriptions to explore ways of measuring compliance
def main():
    load_dotenv()


    if 'snowsesh' not in st.session_state:
            st.session_state.snowsesh = SnowflakeConnection()
            st.session_state.snowsesh.use_database("INTELLIGENCE_DEV")
            st.session_state.snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

    snowsesh = st.session_state.snowsesh


    st.title("Lipid Regulating Drugs (LRDs) Compliance exploration")
    st.write(
        """ This app explores ways to measure medication complaince. It currently specifically foccuses on 
        LRDs for exploration in order to understand the data and it's nuances.

        Still lots of work to do on this, particularly the complaince, switching bit.
        """
    )

    st.subheader("Demographics of those EVER on LRDs")

    query = """ select distinct o.person_id,
    m.gender,
    m.ethnicity,
    m.imd_decile
    from intelligence_dev.ai_centre_dev.lrd_orders o
    LEFT JOIN intelligence_dev.ai_centre_feature_store.person_nel_master_index m 
        ON o.person_id = m.person_id  
    """


    # Fetch the data into a Snowpark DataFrame
    df_demo = snowsesh.execute_query_to_df(query)
    df_demo.columns = df_demo.columns.str.lower()

    gender_data = df_demo['gender'].value_counts()

    # Display the Gender distribution using st.bar_chart
    st.subheader("Gender Distribution")
    st.bar_chart(gender_data)

    # Plot Ethnicity distribution
    ethnicity_data = df_demo['ethnicity'].value_counts()

    # Display the Ethnicity distribution using st.bar_chart
    st.subheader("Ethnicity Distribution")
    st.bar_chart(ethnicity_data)

    imd_data = df_demo['imd_decile'].value_counts().sort_index(ascending=True)

    # Display the Ethnicity distribution using st.bar_chart
    st.subheader("IMD_Decile Distribution")
    st.bar_chart(imd_data)


    st.subheader("Number of different LRDs prescribed per person")
    st.write(
        """ This does not distinguish between concurrent drug use or consecutive drug use
        """
    )

    query = """select *
    from intelligence_dev.ai_centre_dev.lrd_analysis
    """

    # Fetch the data into a Snowpark DataFrame
    df_orders = snowsesh.execute_query_to_df(query)
    df_orders.columns = df_orders.columns.str.lower()

    drugcount_data = df_orders.groupby('person_id', as_index=False)['drug_count'].first()

    # Count occurrences of each unique drug_count value
    drugcount_distribution = drugcount_data['drug_count'].value_counts().sort_index()

    # Display the Ethnicity distribution using st.bar_chart
    st.subheader("Number of different drugs")
    st.bar_chart(drugcount_distribution)

    st.subheader("Top 10 Drugs Prescribed")

    drug_data = df_orders.groupby('drug')['person_id'].nunique()

    # Sort in descending order and keep the top 10
    top_10_drugs = drug_data.sort_values(ascending=False).head(10)

    # Display the top 10 drugs in a bar chart
    st.bar_chart(top_10_drugs)

    st.subheader("PDC by Drug (for top 10 drugs)")
    st.write(""" Proportion Days Covered calculates the number of days
    covered by medication orders, out of the total number of days of the course.
    A course can be estimated by examining gaps in medication orders. See https://joppp.biomedcentral.com/articles/10.1186/s40545-021-00385-w for more details.

    But essentially, you can pick a threshold to say, gaps larger than the threshold will be counted as genuine treatment breaks, 
    and gaps smaller than the threshold will be seen as non adherence. Here we can pick the threshold and see how it changes the PDC per drug.

    Here we look at agregated PDC by drug. PDC = 1 is 100% days overed, >1 is more days covered than required, <1 is lack of coverage

    Please note, some order_durations were set to 0 (clearly an incorrect entry) so these will not be included in the calcuations
    """)

    slider_value = st.slider("Select gap threshold (in days)", min_value=30, max_value=365, value=120, step=1)

    query = f"""
    WITH ordered_orders AS (
        SELECT 
            person_id,
            order_rank,
            drug,
            order_date,
            order_enddate,
            days_to_next_order,
            LEAD(order_date) OVER (PARTITION BY person_id, drug ORDER BY order_rank) AS next_order_date
        FROM intelligence_dev.ai_centre_dev.lrd_analysis
    ),
    periods AS (
        SELECT 
            person_id,
            order_rank,
            drug,
            order_date,
            order_enddate,
            days_to_next_order,
            next_order_date,
            -- Flag the next row if the gap between order_enddate and next_order_date exceeds the slider threshold
            CASE 
                WHEN DATEDIFF(DAY, order_enddate, next_order_date) > {slider_value} THEN 1
                ELSE 0 
            END AS new_period_flag
        FROM ordered_orders
    ),
    periods_with_shifted_flag AS (
        SELECT 
            person_id,
            order_rank,
            drug,
            order_date,
            order_enddate,
            days_to_next_order,
            next_order_date,
            -- Use LAG to shift the flag from current row to the next; for the first row, handle the NULL by setting it to 0
            COALESCE(LAG(new_period_flag) OVER (PARTITION BY person_id, drug ORDER BY order_rank), 0) AS shifted_new_period_flag
        FROM periods
    ),
    periods_with_groups AS (
        SELECT 
            person_id,
            drug,
            order_date,
            order_enddate,
            days_to_next_order,
            next_order_date,
            shifted_new_period_flag,
            -- Calculate period_id by summing shifted_new_period_flag
            SUM(shifted_new_period_flag) OVER (PARTITION BY person_id, drug ORDER BY order_rank ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS period_id
        FROM periods_with_shifted_flag
    )
    SELECT 
        person_id,
        period_id + 1 AS period_id,  -- Add 1 to ensure period starts at 1
        drug,
        MIN(order_date) AS period_start_date,
        MAX(order_enddate) AS period_end_date,
        DATEDIFF(DAY, period_start_date, period_end_date) AS duration_period,
        SUM(CASE 
                WHEN days_to_next_order <= {slider_value} THEN days_to_next_order 
                ELSE 0 
            END) AS order_gaps, 
        (duration_period - order_gaps) as duration_orders,
        CASE WHEN duration_orders = 0 OR duration_period = 0 THEN null
        ELSE duration_orders / duration_period
        END AS est_pdc
    FROM periods_with_groups
    GROUP BY person_id, drug, period_id
    ORDER BY person_id, drug, period_id;
    """

    df= snowsesh.execute_query_to_df(query)
    df.columns = df.columns.str.lower()

    top_10_drugs = top_10_drugs.index.tolist()
    df_filtered = df[df['drug'].isin(top_10_drugs)]
    pdc_summary = df_filtered.groupby('drug').agg(
        duration_orders_sum=('duration_orders', 'sum'),
        duration_period_sum=('duration_period', 'sum')
    )


    # Calculate PDC as duration_orders_sum / duration_period_sum
    pdc_summary['pdc'] = pdc_summary['duration_orders_sum'] / pdc_summary['duration_period_sum']

    st.subheader("Treatment switchers vs concurrent drug users")
    st.write("""It's difficult to accurately ascertain a treatment switch vs concurrent drug prescription accuratly.
    So here we use 2 sliders to help define a swtich vs concurrent use. 

    The first slider will set the threshold for a long gap betwen different medications.
    e.g. if set to 120 days, it will count 2 different drugs prescribed > 120 days apart as a switch, and < 120 days apart as concurrent use.

    The second slider is for if the different drugs are prescribed overlapping. What threshold would we accept as an overlap being possible but still a switch.


    """)

    # Slider for gap between treatments (e.g., 30 days for a switch)
    switch_threshold = st.slider(
        "Select the gap threshold (days) for a treatment switch",
        min_value=1,
        max_value=365,  # Adjust based on your data's typical gap
        value=30,       # Default value (30 days)
        step=1
    )

    # Slider for the small overlap threshold (e.g., 5 days)
    overlap_threshold = st.slider(
        "Select the small overlap threshold (days) for a treatment switch",
        min_value=0,
        max_value=60,  # Typically a smaller threshold, like 0 to 5 or 0 to 30
        value=5,       # Default value (5 days of overlap)
        step=1
    )

    query = f"""
    WITH ordered_orders AS (
        SELECT 
            person_id,
            order_rank,
            drug,
            order_date,
            order_enddate,
            drug_count, -- Assuming drug_count already exists here
            LEAD(drug) OVER (PARTITION BY person_id ORDER BY order_rank) AS next_drug,
            LEAD(order_date) OVER (PARTITION BY person_id ORDER BY order_rank) AS next_order_date,
            DATEDIFF(DAY, order_enddate, LEAD(order_date) OVER (PARTITION BY person_id ORDER BY order_rank)) AS gap_to_next_order
        FROM intelligence_dev.ai_centre_dev.lrd_analysis
        WHERE drug_count >= 2 -- Only consider rows with 2 or more drugs prescribed
    ),
    periods_with_usage_type AS (
        SELECT 
            o.person_id,
            o.drug,
            o.order_date,
            o.order_enddate,
            o.next_drug,
            o.next_order_date,
            o.gap_to_next_order,
        CASE 
        -- Define switchers: Allow slight overlap (negative or small gap), and drug changes
            WHEN o.gap_to_next_order BETWEEN -{overlap_threshold} AND 0 AND o.drug != o.next_drug THEN 'switch'
        
        -- If there is a significant overlap or the gap is small, consider this concurrent
            WHEN o.gap_to_next_order < 0 OR o.gap_to_next_order <= {overlap_threshold} THEN 'concurrent'

        -- If there's a large gap and drugs are different, consider this a treatment switch
            WHEN o.gap_to_next_order > {switch_threshold} AND o.drug != o.next_drug THEN 'switch'

        ELSE 'unknown' -- Fallback
    END AS usage_type
        FROM ordered_orders o
    )
    SELECT
        person_id,
        usage_type,
        drug,
        next_drug,
        COUNT(*) AS drug_count
    FROM periods_with_usage_type
    GROUP BY person_id, usage_type, drug, next_drug
    ORDER BY person_id, usage_type;
    """

    df_classified= snowsesh.execute_query_to_df(query)
    df_classified.columns = df_classified.columns.str.lower()

    usage_counts = df_classified.groupby(['usage_type']).size().reset_index(name='count')

    # Bar chart for the number of switchers vs concurrent users
    st.bar_chart(usage_counts.set_index('usage_type')['count'])

    # Most common switch and concurrent drugs
    most_common_switches = df_classified[df_classified['usage_type'] == 'switch'].groupby(['drug', 'next_drug'])['drug_count'].sum().reset_index(name='count')
    most_common_concurrent = df_classified[df_classified['usage_type'] == 'concurrent'].groupby('drug')['drug_count'].sum().reset_index(name='count')

    # Most common treatment switch (e.g., drug A to drug B)
    if not most_common_switches.empty:
        most_common_switch = most_common_switches.loc[most_common_switches['count'].idxmax()]
        switch_sentence = f"The most common treatment switch is from {most_common_switch['drug']} to {most_common_switch['next_drug']}."
    else:
        switch_sentence = "No significant treatment switch found."

    # Most common concurrently prescribed drugs
    if not most_common_concurrent.empty:
        most_common_concurrent = most_common_concurrent.sort_values('count', ascending=False).head(2)
        concurrent_sentence = f"The most common concurrently prescribed drugs are {', '.join(most_common_concurrent['drug'].values)}."
    else:
        concurrent_sentence = "No significant concurrent drugs found."

    st.write(switch_sentence)
    st.write(concurrent_sentence)


if __name__ == "__main__":
    main()
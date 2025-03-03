import altair as alt
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Multimorbidity Phenotype Analysis", layout="wide")


# Load the data
@st.cache_data
def load_data():
    df_3d = pd.read_csv("ppmi_3d.csv")
    df_2d = pd.read_csv("ppmi_2d.csv", index_col=0)
    max_ppmi_3d = df_3d["ppmi"].max()
    return df_3d, df_2d, max_ppmi_3d


data_3d, data_2d, max_ppmi_3d = load_data()

# Get unique conditions
conditions = sorted(data_2d.columns)

# Create title
st.title("Multimorbidity Phenotype Analysis")

# Create dropdown for condition selection
selected_condition = st.selectbox("Select a condition to view its relationships:", conditions)

# Prepare 2D data for visualization
two_d_data = data_2d[selected_condition].reset_index()
two_d_data.columns = ["condition", "ppmi"]
two_d_data = two_d_data[two_d_data["condition"] != selected_condition]  # Remove self-comparison
two_d_data = two_d_data.sort_values("ppmi", ascending=False)
two_d_data = two_d_data.head(10)

# Create 2D bar chart
st.subheader(f"Pairwise Relationships with {selected_condition}")
bar_chart = (
    alt.Chart(two_d_data)
    .mark_bar()
    .encode(
        y=alt.Y(
            "condition:N", title="Condition", sort="-x", axis=alt.Axis(labelLimit=200)
        ),  # Sort by PPMI value
        x=alt.X("ppmi:Q", title="PPMI Score"),
        color=alt.Color(
            "ppmi:Q", scale=alt.Scale(scheme="viridis", domain=[0, 5]), title="PPMI Score"
        ),
        tooltip=[
            alt.Tooltip("condition:N", title="Condition"),
            alt.Tooltip("ppmi:Q", title="PPMI", format=".3f"),
        ],
    )
    .properties(width=800, height=400)
)

st.altair_chart(bar_chart, use_container_width=True)

# 3D data filtering
filtered_data = data_3d[
    (data_3d["condition3"] == selected_condition)
    & (data_3d["condition1"] != data_3d["condition2"])
    & (data_3d["condition1"] != selected_condition)
    & (data_3d["condition2"] != selected_condition)
    & (data_3d["condition1"] < data_3d["condition2"])
].copy()

# Remove the selected condition from both axes
conditions_subset = [c for c in conditions if c != selected_condition]

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader(f"Three-way PPMI Heatmap for {selected_condition}")
    heatmap = (
        alt.Chart(filtered_data)
        .mark_rect()
        .encode(
            x=alt.X("condition1:N", title="Condition 1", sort=conditions_subset),
            y=alt.Y("condition2:N", title="Condition 2", sort=conditions_subset),
            color=alt.Color(
                "ppmi:Q", scale=alt.Scale(scheme="viridis", domain=[0, 5]), title="PPMI Score"
            ),
            tooltip=[
                alt.Tooltip("condition1:N", title="Condition 1"),
                alt.Tooltip("condition2:N", title="Condition 2"),
                alt.Tooltip("ppmi:Q", title="PPMI", format=".3f"),
                alt.Tooltip("count:Q", title="Count", format=","),
            ],
        )
        .properties(width=600, height=600)
    )
    st.altair_chart(heatmap, use_container_width=True)

with col2:
    st.subheader("Top 10 Strongest Relationships")
    top_relationships = filtered_data.nlargest(10, "ppmi")
    top_relationships = top_relationships[["condition1", "condition2", "ppmi", "count"]]
    top_relationships = top_relationships.round({"ppmi": 3})
    st.dataframe(top_relationships, height=400)

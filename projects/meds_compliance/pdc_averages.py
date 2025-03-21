import pandas as pd
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection


def main():
    # Load environment variables
    load_dotenv()

    # Establish Snowflake connection
    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_DEV")

    # Define the query
    query = """
    SELECT *
    FROM
        INTELLIGENCE_DEV.AI_CENTRE_DEV.COMP_LTC_PDC
    """
    # Execute query and get the DataFrame
    snowpark_df = snowsesh.execute_query_to_df(query)
    snowsesh.preview_table("COMP_LTC_PDC")

    # Check if data was returned
    if snowpark_df.empty:  # This checks if the list is empty
        print("No data returned from Snowflake query.")
        return

    avg_compliance = snowpark_df.groupby("MEDICATION_COMPLIANCE").agg(
        computed_compliance_score=pd.NamedAgg(
            column="TOTAL_DURATION",
            aggfunc=lambda x: (
                None if x.sum() == 0 else
                snowpark_df.loc[x.index, "TOTAL_DURATION_WITH_ORDERS"].sum() / x.sum()
                )
        )
    ).reset_index()

    print(snowpark_df.dtypes)
    print(avg_compliance)

if __name__ == "__main__":
    main()

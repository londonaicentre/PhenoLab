import re

import numpy as np
import pandas as pd
from dotenv import load_dotenv
import scipy 

from phmlondon.snow_utils import SnowflakeConnection

# Generates INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.cleaned_observations
"""
This is a script that creates a cleaned version of the observations table, initially just HbA1c
There are a large number of units and also impossible/ unrealistic data points in the HbA1c table
I initially converted from percent (DCCT) to mmol/mol (IFCC)
Due to the shape of the distribution and the appearance of lots of impossible values, but also a lot of values
that appear to have been input incorrectly (for example appear to be percentages but input as mmol/mol) it 
was difficult to use statistical methodology to calculate this. Therefore I inspected the data and generated cutoffs
I crossreferenced with the maximum GSTT lab values (196), and the minimum reasonable from the A1c to glucose conversion
(17 would be an average glucose of 3.3, which is just about possible, although suggests high cell turnover)
However potentially a better approach would be to define a distribution (this one looks like it is a skewed normal)
fit the distribution and then define outliers. TBC next week
"""


initial_query_top = """
SELECT 
obs.result_value,
obs.result_value_units,
CASE 
when REGEXP_LIKE(obs.result_value_units, '.*(%|per[ -]?cent|DCCT).*', 'i') then (obs.result_value - 2.15)*10.929
when REGEXP_LIKE(obs.result_value_units, '.*(mmol|mM/M|IFCC).*', 'i') then obs.result_value 
END 

as cleaned_result_value,"""

query_tail = """
'mmol/mol' as cleaned_result_value_units,
obs.id,
obs.organization_id, 
obs.patient_id,
obs.person_id,
obs.encounter_id,
obs.date_recorded,
obs.core_concept_id,
def.code_description,
def.definition_name,
def.definition_version,
def.definition_source,
def.version_datetime
FROM PROD_DWH.ANALYST_PRIMARY_CARE.OBSERVATION obs
left join intelligence_dev.ai_centre_definition_library.definitionstore def on obs.core_concept_id = def.dbid
where cleaned_result_value is not null
and cleaned_result_value_units is not null
and def.code is not null
and def.DEFINITION_NAME = 'hba1c_definition_gp'
and NOT REGEXP_LIKE(obs.result_value_units, '.*mg.*', 'i')
"""

def main():
    load_dotenv()


    try:
        snowsesh = SnowflakeConnection()
        snowsesh.use_database("INTELLIGENCE_DEV")
        snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")

        #Pull the less cleaned data out - plan will be to better model hba1c, in the meantime we can use this
        #initial_df = snowsesh.execute_query_to_df(initial_query_top + query_tail)


        main_query_top = """
        CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.cleaned_hba1c AS
        SELECT 
        obs.result_value,
        obs.result_value_units,
            CASE 
                WHEN (CASE 
                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(%|per[ -]?cent|DCCT).*', 'i') 
                        THEN (obs.result_value - 2.15) * 10.929  
                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(mmol|mM/M|IFCC).*', 'i') 
                        THEN obs.result_value 
                    END) < 3 THEN NULL
                WHEN (CASE
                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(%|per[ -]?cent|DCCT).*', 'i') 
                        THEN (obs.result_value - 2.15) * 10.929  
                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(mmol|mM/M|IFCC).*', 'i') 
                        THEN obs.result_value 
                                    END) < 15 THEN (CASE 
                                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(%|per[ -]?cent|DCCT).*', 'i') 
                                        THEN (obs.result_value - 2.15) * 10.929  
                                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(mmol|mM/M|IFCC).*', 'i') 
                                        THEN obs.result_value 
                                    END) * 10

                WHEN (CASE 
                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(%|per[ -]?cent|DCCT).*', 'i') 
                        THEN (obs.result_value - 2.15) * 10.929  
                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(mmol|mM/M|IFCC).*', 'i') 
                        THEN obs.result_value 
                    END) >18 then (CASE 
                                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(%|per[ -]?cent|DCCT).*', 'i') 
                                        THEN (obs.result_value - 2.15) * 10.929  
                                        WHEN REGEXP_LIKE(obs.result_value_units, '.*(mmol|mM/M|IFCC).*', 'i') 
                                        THEN obs.result_value 
                                    END)
            END AS cleaned_result_value,
        """

        snowsesh.execute_query(main_query_top + query_tail)

    except Exception as e:
        print(f"Error creating feature store: {e}")
        raise e
    finally:
        snowsesh.session.close()

if __name__ == "__main__":
    main()

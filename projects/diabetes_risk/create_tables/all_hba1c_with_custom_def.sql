with hba1cs as (
    select o.*, c.name
    from prod_dwh.analyst_primary_care.observation as o
    join prod_dwh.analyst_primary_care.concept as c
    on o.core_concept_id = c.dbid
    join intelligence_dev.ai_centre_definition_library.definitionstore as ds
    on c.code = ds.code
    where ds.definition_id = '1aa77156'
    -- and o.person_id = 51490
),
corrected_hba1cs as (
    select *,
        CASE
            WHEN result_value BETWEEN 4 AND 15 THEN '%'
            WHEN result_value BETWEEN 19 and 140 THEN 'mmol/mol'
            ELSE 'invalid'
        END AS result_value_inferred_units,
        CASE
            WHEN result_value BETWEEN 4 and 15 THEN ROUND((result_value*10.93) - 23.5, 2)
            WHEN result_value BETWEEN 19 and 140 THEN result_value
            ELSE NULL -- invalid values set to null
        END AS result_value_cleaned_and_converted_to_mmol_per_mol
    from hba1cs),
tidied_values as (
    select person_id, 
        clinical_effective_date, 
        age_at_event,
        result_value as result_value_raw,
        result_value_cleaned_and_converted_to_mmol_per_mol 
    from corrected_hba1cs
    where result_value_cleaned_and_converted_to_mmol_per_mol is not null
    and (
        clinical_effective_date < DATE '2024-01-01'
        and clinical_effective_date > DATE '2025-01-31'
    ) -- advice from Jordan: faulty laboratory: advised to remove all values from 2024-01-01 to 2025-01-31 as 
    -- it is unknown how to trace affected values
)
select *
from tidied_values;
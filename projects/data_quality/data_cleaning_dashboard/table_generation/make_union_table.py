import json


def main():
    #Pull out the config file
    with open('value_cutoffs/cutoffs.json', 'r+') as connection:
        cutoffs = json.load(connection)
    tables = [i for i in cutoffs.keys()]

    #Make union table
    query = f"""
    CREATE OR REPLACE TABLE INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.OBSERVATIONS_CLEAN AS
    SELECT *
    FROM (
        {''.join('SELECT * FROM INTELLIGENCE_DEV.' + cutoffs[table]['schema'] + '.' + table + '_VALUE_CUTOFFS' +
                 '\n\t\t\tUNION ALL\n' for table in tables[0:-1])}
        SELECT * FROM INTELLIGENCE_DEV.{cutoffs[tables[-1]]['schema']}.{tables[-1]}_VALUE_CUTOFFS
    ) ;
    """
    with open('union_all_cleaned_observations.sql', 'w') as file:
            file.write(query)

    print('Made union all table')

if __name__ == "__main__":
    main()

import warnings
from dataclasses import dataclass

import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection


class DataQuality:
    def __init__(self, connection: SnowflakeConnection, database: str, schema: str, df_type: str = 'pd') -> None:
        """
        Set up a connection and the files we are looking at
        Choose a database manager - default is pandas, supports polars, needs to be in format either pd or pl
        - Note that the functions may behave slightly differently depending on if you use polars or pandas
        """
        self.conn = connection
        self.conn.current_database = database.upper()
        self.conn.current_schema = schema.upper()
        self.df_type = df_type
        self.schema_list = self.show_schemas()
        self.database_list = self.show_databases()
        self.table_list = self.show_tables()
        self._current_table = None
        self.column = None

    def execute_query_to_table(self, query: str) -> pl.DataFrame | pd.DataFrame:
        """Function to return a dataframe from a sql query
        - able to flexibly return either a polars or pandas df depending on settings
        query: str : this is the sql query that we are running"""
        if self.df_type == 'pd':
            return self.conn.execute_query_to_df(query)
        elif self.df_type == 'pl':
            return self.conn.execute_query_to_polars(query)

    #Set up some properties for the database, schema and table stuff
    @property
    def current_database(self) -> str:
        return self.conn.current_database

    @current_database.setter
    def current_database(self, database: str) -> None:
        if database.upper() not in self.database_list:
            raise ValueError(f'Database must in list of databases: {[i for i in self.database_list]}')
        else:
            self.conn.current_database = database.upper()

        #Make sure it is clear that the database needs to match the schema
        self.schema_list = self.show_schemas()
        if self.current_schema not in self.schema_list:
            warnings.warn(f'Current schema {self.current_schema} not in database {database}. '
                          f'Schema needs to be one of {[i for i in self.schema_list]}.'
                          'current_table and current_column will also need to be updated',
                          stacklevel=2)

    @property
    def current_schema(self) -> str:
        return self.conn.current_schema

    @current_schema.setter
    def current_schema(self, schema: str) -> None:
        self.table_list = self.show_tables()
        if schema.upper() not in self.schema_list:
            raise ValueError(f'Schema must in list of schemas within {self.current_database}:\n'
                             f' {[i for i in self.schema_list]}')
        else:
            self.conn.current_schema = schema.upper()

        self.table_list = self.show_tables()
        if self.current_table not in self.table_list:
            warnings.warn(f'Current table {self.current_table} not in schema {schema}. '
                          f'Table needs to be one of {[i for i in self.table_list]}.'
                          'current_column will also need to be updated',
                          stacklevel=2)

    @property
    def current_table(self) -> str:
        return self._current_table

    @current_table.setter
    def current_table(self, table: str) -> None:
        if table is not None:
            self.column_list = self.show_columns(table)
            if table.upper() not in self.table_list:
                raise ValueError(f'Table must in list of tables within {self.current_schema}:\n'
                                f' {[i for i in self.table_list]}')
            else:
                self._current_table = table.upper()

            self.column_list = self.show_columns(table)
            if self._current_table not in self.table_list:
                warnings.warn(f'Column {self.current_column} not in table {table}. '
                            f'Column needs to be one of {[i for i in self.column_list]}.',
                            stacklevel=2)

    @property
    def current_column(self) -> str:
        return self.current_column

    @current_column.setter
    def current_column(self, column: str) -> None:
        if column is not None:
            self.column_list = self.show_columns() #Update the table list if we change the schema
            if column.upper() not in self.column_list:
                raise ValueError(f'Column must in list of columns within {self.current_table}:\n'
                                f' {[i for i in self.column_list]}')
            else:
                self.current_column = column.upper()

    def show_tables(self, all: bool = False) -> pd.Series:
        if not all:
            query = f'show tables in schema {self.conn.current_database}.{self.conn.current_schema}'
        else:
            query = f'show tables in database {self.conn.current_database}'

        #Unfortunately we have to use pandas here because the polars method doesn't work with show tables
        try:
            self.table_list = [i for i in self.conn.execute_query_to_df(query).name]
            return self.table_list
        except Exception as e:
            print(e)


    def long_table(self, table: str) -> str:
        return f'{self.conn.current_database}.{self.conn.current_schema}.{table}'

    def show_schemas(self) -> pd.Series:
        query = f'show schemas in database {self.conn.current_database}'
        return [i for i in self.conn.execute_query_to_df(query).name]

    def show_databases(self) -> pd.Series:
        return [i for i in self.conn.execute_query_to_df('show databases').name]

    def show_columns(self, table: str) -> pd.Series:
        query = f'select * from {self.long_table(table)} limit 1'
        return [i for i in self.conn.execute_query_to_df(query).columns]

    def dtype(self, table: str, column: str):
        query = f'DESCRIBE TABLE {self.long_table(table)}'
        tab = self.execute_query_to_table(query)
        return tab[tab.name == column.upper()].type

    def count_null(self, table: str, column: str):
        null_query = f'SELECT COUNT({column}) as null_count FROM {self.long_table(table)}'
        tab =  self.execute_query_to_table(null_query)
        return tab.NULL_COUNT

    def table_length(self, table: str):
        length_query = f'SELECT COUNT(*) as total_count FROM {self.long_table(table)}'
        tab = self.execute_query_to_table(length_query)
        return tab.TOTAL_COUNT

    def proportion_null(self, table: str, column: str):
        return self.count_null(table, column)/ self.table_length(table)

    def mean(self, table: str, column: str) -> float:
        mean_query = f'SELECT AVG({column}) FROM {self.long_table(table)}'
        return self.execute_query_to_table(mean_query)
    
    def wrong_dtype(self, table: str, column: str):
        print('wrong_dtype')

def main():
    load_dotenv()

    snowsesh = SnowflakeConnection()
    snowsesh.use_database("INTELLIGENCE_DEV")
    snowsesh.use_schema("AI_CENTRE_FEATURE_STORE")
    #snowsesh = ExtendedSnowflakeConnection(SnowflakeConnection(), "INTELLIGENCE_DEV", "AI_CENTRE_FEATURE_STORE")

    source_query = """
    select *
    from INTELLIGENCE_DEV.AI_CENTRE_FEATURE_STORE.PERSON_NEL_MASTER_INDEX
    LIMIT 100000
    """
    data_qual = DataQuality(SnowflakeConnection, 'INTELLIGENCE_DEV', 'AI_CENTRE_FEATURE_STORE')
    tabs = data_qual.show_tables()
    cols = data_qual.show_columns('cohort_table')
    data_qual.mean('cohort_table', 'LOS_UNADJUSTED_DAYS')
    data_qual.dtype('cohort_table', 'date_of_birth')
    data_qual.proportion_null('cohort_table', 'admission_time')
    one_col = data_qual.OneColumn('cohort_table', 'person_id', data_qual)
    initial_data_pl = snowsesh.execute_query_to_polars(source_query)
    initial_data_df = snowsesh.execute_query_to_df(source_query)
    initial_data_df.dtypes
    initial_data_pl.dtypes

if __name__ == '__main__':
    main()

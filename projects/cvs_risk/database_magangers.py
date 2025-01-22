from abc import ABC, abstractmethod
from phenotype_class import Phenotype
import sqlite3
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
from tables import TableQueries
from snowflake.snowpark import Row

class DatabaseManager(ABC):
    """Abstract class to create a SQL phenotypes table and add phenotypes to it"""
    @abstractmethod
    def _create_table(self):
        print("Create table method called")

    @abstractmethod
    def add_phenotype(self, phenotype: Phenotype):
        print(f"Phenotype {Phenotype} added to database")

    @abstractmethod
    def get_all_phenotypes(self) -> list:
        pass

class LocalDatabaseManager(DatabaseManager):
    """Class to use SQLite to create a local database - use for development"""
    def __init__(self, database_filename: str):
        self.conn = sqlite3.connect(database_filename)
        self._create_table()

    def _create_table(self):
        cursor = self.conn.cursor()
        cursor.execute(TableQueries.queries['cvs_risk_phenotypes'])
        self.conn.commit()
        cursor.close

    def add_phenotype(self, phenotype: Phenotype):
        """Takes the data in a Phenotype object and adds it to the database"""
        phenotype.df.to_sql("phenotypes", self.conn, if_exists="append",
                            index=False)
        super().add_phenotype(phenotype)
        #  Note that currently the phenotype will be readded the database each
        #  time this method is called for the same phenotype

    def get_all_phenotypes(self) -> list[tuple]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM phenotypes")
        rows = cursor.fetchall()
        cursor.close
        return rows

class SnowflakeDatabaseManager(DatabaseManager):
    """Class for creating table and adding phenotypes on snowflake"""
    def __init__(self, database: str, schema: str):
        load_dotenv()
        self.snowconnection = SnowflakeConnection()
        self.snowconnection.use_database(database)
        self.snowconnection.use_schema(schema)
        self.snowsesh = self.snowconnection.session
        self._create_table()

    def _create_table(self):
        super()._create_table()
        self.table_name = 'cvs_risk_phenotypes'
        self.snowsesh.sql(TableQueries.queries['cvs_risk_phenotypes']).collect()

    def add_phenotype(self, phenotype: Phenotype):
        phenotype.df.columns = phenotype.df.columns.str.upper()
        # snowflake converts any table and column names that aren't in quotes to uppercase. But for some reason doesn't do this
        # when creating a datframe from a pandas df in the line below. So we need to manually convert to uppercase otherwise 
        # appending the data to the table fails due to case
        snowpark_df = self.snowsesh.create_dataframe(phenotype.df) # converts pandas dataframe to snowpark dataframe (confusing)
        snowpark_df.show()
        snowpark_df.write.save_as_table(self.table_name, mode="append", column_order="name")

    def get_all_phenotypes(self) -> list[Row]:
        df = self.snowsesh.table(self.table_name)
        rows = df.collect()
        return rows
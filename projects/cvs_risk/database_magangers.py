from abc import ABC, abstractmethod
from phenotype_class import Phenotype
import sqlite3
from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection

class DatabaseManager(ABC):
    """Abstract class to create a SQL phenotypes table and add phenotypes to it"""
    @abstractmethod
    def _create_table(self):
        pass

    @abstractmethod
    def add_phenotype(self, phenotype: Phenotype):
        pass

    @abstractmethod
    def get_all_phenotypes(self):
        pass

class LocalDatabaseManager(DatabaseManager):
    """Class to use SQLite to create a local database - use for development"""
    def __init__(self, database_filename: str):
        self.conn = sqlite3.connect(database_filename)
        self._create_table()

    def _create_table(self):
        print("Create table method called")
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS phenotypes (
                record_id INTEGER PRIMARY KEY,
                phenotype_id VARCHAR(30) NOT NULL,
                phenotype_version VARCHAR(30),
                phenotype_name VARCHAR(50),
                concept_code VARCHAR(30),
                coding_system VARCHAR(30),
                clinical_code VARCHAR(50),
                code_description VARCHAR(255)
            )
        """
        )
        self.conn.commit()
        cursor.close

    def add_phenotype(self, phenotype: Phenotype):
        """Takes the data in a Phenotype object and adds it to the database"""
        phenotype.df.to_sql("phenotypes", self.conn, if_exists="append",
                            index=False)
        print("Phenotype added to database")
        #  Note that currently the phenotype will be readded the database each
        #  time this method is called for the same phenotype

    def get_all_phenotypes(self) -> list[tuple]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM phenotypes")
        rows = cursor.fetchall()
        cursor.close
        return rows  # may need tidying - consider this as a TODO once snowflake connected

class SnowflakeDatabaseManager(DatabaseManager):
    """Class for creating table and adding phenotypes on snowflake"""
    def __init__(self, database: str, schema: str):
        super().__init__()
        load_dotenv()
        self.snowsesh = SnowflakeConnection()
        self.snowsesh.use_database(database)
        self.snowsesh.use_schema(schema)

    def _create_table(self):
        # TODO: create the table

    def add_phenotype(self, phenotype: Phenotype):
        # TODO

    def get_all_phenotypes(self):
        # TODO 
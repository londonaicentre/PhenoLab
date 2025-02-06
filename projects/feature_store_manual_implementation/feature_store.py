from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
import logging # optional, for debugging
# from sql_metadata import Parser

# logging.basicConfig(level=logging.DEBUG) # optional, for debugging

load_dotenv()
conn = SnowflakeConnection()
    
class FeatureStoreManager:
    def __init__(self, 
                 connection: SnowflakeConnection, 
                 database: str, 
                 schema: str):
        """
        The schema where you want to create the feature store should already exist
        """
        self.conn = connection
        self.database = database
        self.schema = schema
        self.conn.use_database(self.database)
        self.conn.use_schema(self.schema)
    
    def create_feature_store(self):
        """
        Creates the FEATURE_REGISTRY if it doesn't exist.
        Raises an Exception if the table already exists.
        """
        session = self.conn.session
        self.table_names = table_names = ['FEATURE_REGISTRY', 'FEATURE_VERSION_REGISTRY', 'LIVE_FEATURES']

        self._check_table_exists(table_names[0])
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_names[0]} (
                    feature_id PRIMARY KEY,
                    feature_name VARCHAR NOT NULL,
                    feature_desc VARCHAR,
                    feature_format VARCHAR,
                    table_name VARCHAR,
                    date_feature_registered DATETIME,
                    ) """).collect()
        print(f"{table_names[0]} created successfully.")

        self._check_table_exists(table_names[1])
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_names[1]} (
                feature_ID INT,
                feature_version INT,
                table_name VARCHAR,
                sql_query TEXT,
                live_updating BOOLEAN,
                change_description VARCHAR,
                date_version_registered DATETIME,
            ) """).collect()
        print(f"{table_names[1]} created successfully.")

        self._check_table_exists(table_names[2])
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_names[2]} (
                    feature_id INT,
                    feature_version INT,
                    model_id INT,
                    model_version INT,
                    date_registered_as_live DATETIME,
                    """) # every active model should register a new entry, even if using the same feature
        print(f"{table_names[2]} created successfully.")

    def add_new_feature(self, 
                        feature_name: str, 
                        feature_desc: str, 
                        feature_format: str,
                        sql_select_query_to_generate_feature: str,
                        target_lag: str = '1 day') -> tuple[int, int]:
        """
        Executes the input query and add the feature to the feature registry and the version registry
        feature_name: name of the feature, str
        feature_desc: description of the feature, str
        feature_format: format of the feature e.g. one-hot, binary, continuous; str
        table_name: table_name of the feature, str
        sql_select_query_to_generate_feature: SQL query that generates the feature, str
        target_lag: how often the feature should refresh from the underlying tables e.g. '5 minutes', '1 hour', '1 day'. Default is '1 day'

        """
        session = self.conn.session

        # Add the feature to the feature registry
        feature_id = session.sql(f"""
            INSERT INTO feature_registry (
                    feature_name, 
                    feature_desc, 
                    feature_format, 
                    date_feature_registered)
            VALUES ('{feature_name}', '{feature_desc}', '{feature_format}', CURRENT_TIMESTAMP)
            RETURNING feature_id
        """).collect() # vulnerable to SQL injection - never expose externally
        print(f'Feature {feature_name}, ID {feature_id}, added to the feature registry; table not created yet')

        try:
            table_name = self._create_feature_table(feature_name, feature_id, target_lag, sql_select_query_to_generate_feature)
            feature_version = self._add_new_feature_version_to_version_registry(feature_id, 
                                                                                table_name, 
                                                                                sql_select_query_to_generate_feature,
                                                                                target_lag, 
                                                                                "Initial version")

        except Exception as e:
            print(f"Error creating table: {e}. Registry entry will now be deleted")
            session.sql(f"""
                DELETE FROM feature_registry
                WHERE feature_id = {feature_id}
            """).collect()
            raise e

        return feature_id, feature_version

    def _check_table_exists(self, 
                            table_name: str):

        session = self.conn.session
        if session.sql(f"SHOW TABLES LIKE {table_name}").collect():
            raise ValueError(f"{table_name} already exists.")

    def _create_table_creation_query_from_select_query(self, 
                                                       table_name: str, 
                                                       target_lag: str, 
                                                       select_query: str) -> str:

        full_query = f"""
                CREATE DYNAMIC TABLE {table_name}
                TARGET_LAG = '{target_lag}'
                WAREHOUSE = INTELLIGENCE_XS
                AS
                {select_query}
            """   
        return full_query
    
    def _get_feature_table_name(self,
                                feature_name: str,
                                feature_id: int) -> str:
        feature_version = self._get_current_feature_version(feature_id)[1]
        return self._table_naming_convention(feature_name, feature_version)
    
    def _table_naming_convention(self,
                                 feature_name: str,
                                 feature_version: int) -> str:
        return f"{feature_name}_v{feature_version}"

    def _create_feature_table(self,
                             feature_name: str,
                             feature_id: int,
                             target_lag: str,
                             select_query: str):
        
        session = self.conn.session
        table_name = self._get_feature_table_name(feature_name, feature_id)

        # Quality checks
        self._check_table_exists(table_name)
        if table_name in self.table_names:
            raise ValueError(f"{table_name} is a reserved name.")
            
        # Generate the full sql query
        full_query = self._create_table_creation_query_from_select_query(table_name, target_lag, select_query)
        
        # Create the table
        session.sqr(full_query).collect()

        print(f"Table {table_name} created successfully")

        return table_name

    
    def _get_current_feature_version(self, 
                                     feature_id: int) -> tuple[int, int]:
        session = self.conn.session

        result = session.sql(f"""
                SELECT COALESCE(MAX(feature_version), 0) AS max_version
                FROM feature_version_registry
                WHERE feature_id = {feature_id}
                """).collect()
        current_version = result[0]['MAX_VERSION']
        next_version = current_version + 1

        return current_version, next_version

    def _add_new_feature_version_to_version_registry(self, 
                                 feature_id: int,
                                 table_name: str, 
                                 sql_query: str,
                                 lag: str, 
                                 change_description: str) -> int:
            session = self.conn.session

            feature_version = self._get_current_feature_version(feature_id)[1]

            live_updating = True #hardcode that whenevr a new feature is added, it is live updating

            session.sql(f"""INSERT INTO feature_version_registry (
                        feature_ID,
                        feature_version,
                        table_name,
                        sql_query,
                        live_updating,
                        lag, 
                        change_description, 
                        date_version_registered)
                VALUES (
                    ({feature_id},
                    {feature_version},
                    {table_name},
                    '{sql_query}',
                    '{lag}',
                    '{change_description}',
                    CURRENT_TIMESTAMP)
            """).collect()

            print(f"Feature {feature_id} version {feature_version} added to the feature version registry")

            return feature_version

    def update_feature(self, 
                       feature_id :int, 
                       new_sql_select_query: str, 
                       change_description: str,
                       target_lag: str = ''):
        """
        Updates the feature in the feature version registry with the new query and description
        feature_id: id of the feature, int
        new_sql_select_query: new SQL query that generates the feature, str
        change_description: description of the change, str
        """
        session = self.conn.session

        if not session.sql(f"""SELECT feature_id FROM feature_registry WHERE feature_id = {feature_id}""").collect():
            raise ValueError(f"Feature {feature_id} does not exist.")
        else:
            feature_name = session.sql(f"""SELECT feature_name FROM feature_registry WHERE feature_id = {feature_id}""").collect()
            print(f"Updating feature {feature_name}, with ID {feature_id}")
    

        if not target_lag:
            # if no target lag given to the update function, use the same lag as the most recent
            target_lag = session.sql(f"""
                SELECT lag
                FROM feature_version_registry
                WHERE feature_id = {feature_id}
                ORDER BY date_version_registered DESC
                LIMIT 1
            """).collect()[0]['LAG']

        # Create new feature and add to registry
        table_name = self._create_feature_table(feature_name, feature_id, target_lag, new_sql_select_query)
        feature_version = self._add_new_feature_version_to_version_registry(feature_id, table_name, new_sql_select_query, target_lag, change_description)

        # Stop live updating on the old version of the feature
        old_version = feature_version - 1
        table_name = session.sql(f"""
                                 SELECT table_name FROM feature_version_registry 
                                 WHERE feature_id = {feature_id} AND feature_version = {old_version}""").collect()
        session.sql(f"""ALTER DYNAMIC TABLE {old_table_name} SUSPEND: """).collect() # existing data still exists
        # note dynamic tables can be resumed if wanted
        session.sql(f"""UPDATE feature_version_registry SET live_updating = FALSE WHERE feature_id = {feature_id} AND feature_version = {old_version}""").collect()
    

    # def get_data

    # def deprecate_feature

    #  serve current version of feature
    # data drift


if __name__ == "__main__":
    DATABASE = "INTELLIGENCE_DEV"
    SCHEMA = "TEST_FEATURE_STORE_IW_2"
    feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)
    feature_store_manager.create_feature_store()
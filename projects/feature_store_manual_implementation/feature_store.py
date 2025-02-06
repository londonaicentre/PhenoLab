from dotenv import load_dotenv
from phmlondon.snow_utils import SnowflakeConnection
import logging # optional, for debugging
# from sql_metadata import Parser

# logging.basicConfig(level=logging.DEBUG) # optional, for debugging

load_dotenv()
conn = SnowflakeConnection()
    
class FeatureStoreManager:
    def __init__(self, connection: SnowflakeConnection, database: str, schema: str):
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
        self.table_names = table_names = ['FEATURE_REGISTRY', 'FEATURE_VERSION_REGISTRY', '{table_names[2]}']
        try:
            self._check_table_exists(table_names[0])
            master_table_exists = session.sql(f"SHOW TABLES LIKE {table_names[0]}").collect()
            if not master_table_exists:
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
            else:
                raise ValueError(f"{table_names[0]} already exists.")

            feature_version_table_exists = session.sql(f"SHOW TABLES LIKE {table_names[1]}'").collect()
            if not feature_version_table_exists:
                session.sql(f"""
                    CREATE TABLE IF NOT EXISTS {table_names[1]} (
                        feature_ID INT,
                        feature_version INT,
                        sql_query TEXT,
                        change_description VARCHAR,
                        date_version_registered DATETIME,
                    ) """).collect()
                print(f"{table_names[1]} created successfully.")
            else:
                raise ValueError(f"{table_names[1]} already exists.")
            
            live_feature_table_exists = session.sql(f"SHOW TABLES LIKE {table_names[2]}").collect()
            if not live_feature_table_exists:
                session.sql(f"""
                    CREATE TABLE IF NOT EXISTS {table_names[2]} (
                            feature_id INT,
                            feature_version INT,
                            model_id INT,
                            model_version INT,
                            date_registered_as_live DATETIME,
                            """) # every active model should register a new entry, even if using the same feature
                print(f"{table_names[2]} created successfully.")
            else:
                raise ValueError(f"{table_names[2]} already exists.")

                            
        except Exception as e:
            print(e)

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
        try:
            # parsed_query = Parser(generate_query)
            # table_names = parsed_query.tables # returns list of strings of table names
            
            # if    table_name not in table_names:
            #     raise ValueError(f"Feature location   table_name} does not seem to be created by your query.")

            table_name = feature_name + '1' #hard-coding that this is the first version, which is probably not ideal as check programatically later on

            feature_table_exists = session.sql(f"SHOW TABLES LIKE {table_name}").collect()
            if feature_table_exists:
                raise ValueError(f"Feature table {table_name} already exists.")
            if table_name in self.table_names:
                raise ValueError(f"Feature table name {table_name} is a reserved name.")
            
            # Generate the full sql query
            full_query = self._create_table_creation_query_from_select_query(table_name, target_lag, sql_select_query_to_generate_feature)
            
            # Create the table
            session.sqr(full_query).collect()

            # Add the feature to the feature registry
            feature_id = session.sql(f"""
                INSERT INTO feature_registry (
                        feature_name, 
                        feature_desc, 
                        feature_format, 
                        table_name,
                        date_feature_registered)
                VALUES ('{feature_name}', '{feature_desc}', '{feature_format}', '{table_name}', CURRENT_TIMESTAMP)
                RETURNING feature_id
            """).collect() # vulnerable to SQL injection - never expose externally

            feature_version = self._add_new_feature_version(feature_id, full_query, "Initial version")

            return feature_id, feature_version

        except Exception as e:
            print(e)

    def _check_table_exists(self, table_name: str):

        session = self.conn.session
        table_exists = session.sql(f"SHOW TABLES LIKE {table_name}").collect()
        raise ValueError(f"{table_name} already exists.") if table_exists else False

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

    def _add_new_feature_version(self, feature_id: int, sql_query: str, change_description: str) -> int:
            session = self.conn.session

            # Create the first version of the feature in the feature version registry
            result = session.sql(f"""
                    SELECT COALESCE(MAX(feature_version), 0) AS max_version
                    FROM feature_version_registry
                    WHERE feature_id = {feature_id}
                    """).collect()
            current_max = result[0]['MAX_VERSION']
            feature_version = current_max + 1

            session.sql(f"""INSERT INTO feature_version_registry (
                        feature_ID,
                        feature_version,
                        sql_query, 
                        change_description, 
                        date_version_registered)
                VALUES (
                    ({feature_id},
                    {feature_version},
                    '{sql_query}',
                    '{change_description}',
                    CURRENT_TIMESTAMP)
            """).collect()

            return feature_version

    def update_feature(self, feature_id :int, new_sql_select_query: str, change_description: str):
        """
        Updates the feature in the feature version registry with the new query and description
        feature_id: id of the feature, int
        new_sql_select_query: new SQL query that generates the feature, str
        change_description: description of the change, str
        """
        session = self.conn.session
        try:
            self._add_new_feature_version(feature_id, new_sql_select_query, change_description)

            # create new feature
            # suspend old one
            # update feature version registry

        

    # def upate_feature

    # def get_data

    # def deprecate_feature

    #  serve current version of feature
    # data drift


if __name__ == "__main__":
    DATABASE = "INTELLIGENCE_DEV"
    SCHEMA = "TEST_FEATURE_STORE_IW_2"
    feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)
    feature_store_manager.create_feature_store()
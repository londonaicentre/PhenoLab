"""
Feature store manager class for managing a snowflake schema that will be used as a feature stoe, containing static 
tables of features plus metadata tables to manage versioning and record the query that created them.
"""

from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

class FeatureStoreManager:
    def __init__(self, connection: SnowflakeConnection, database: str, schema: str, metadata_schema: str = None):
        """
        The schema where you want to create the feature store should already exist

        Args:
            connection (SnowflakeConnection): a connection to the snowflake database
            database (str): the database where the feature store will be created (already exists)
            schema (str): the schema where the feature store will be created (already exists)
        """
        self.conn = connection
        self.database = database
        self.schema = schema
        if metadata_schema is None:
            metadata_schema = schema
        self.metadata_schema = metadata_schema
        self.conn.use_database(self.database)
        self.conn.use_schema(self.schema)
        self.table_names = ["FEATURE_REGISTRY", "FEATURE_VERSION_REGISTRY", "FEATURES_ACTIVE"]

    def create_feature_store(self):
        """
        Creates the FEATURE_REGISTRY if it doesn't exist.
        Raises an Exception if the table already exists.
        """
        table_names = self.table_names

        self.conn.use_schema(self.metadata_schema)
        session = self.conn.session
        self._check_table_exists(table_names[0], self.metadata_schema)
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_names[0]} (
                    feature_id STRING DEFAULT UUID_STRING(),
                    feature_name VARCHAR NOT NULL,
                    feature_desc VARCHAR,
                    feature_format VARCHAR,
                    date_feature_registered TIMESTAMP
                    ) """).collect()
        print(f"{table_names[0]} created successfully.")

        self._check_table_exists(table_names[1], self.metadata_schema)
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_names[1]} (
                feature_ID STRING,
                feature_version INT,
                table_name VARCHAR,
                sql_query TEXT,
                change_description VARCHAR,
                date_version_registered TIMESTAMP
            ) """).collect()
        print(f"{table_names[1]} created successfully.")

        self._check_table_exists(table_names[2], self.metadata_schema)
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_names[2]} (
                    feature_id STRING,
                    feature_version INT,
                    model_id INT,
                    model_version INT,
                    date_registered_as_active TIMESTAMP
                )  """).collect()  # every active model should register a new entry, even if using the same feature
        print(f"{table_names[2]} created successfully.")
        self.conn.use_schema(self.schema)

    def add_new_feature(
        self,
        feature_name: str,
        feature_desc: str,
        feature_format: str,
        sql_select_query_to_generate_feature: str,
        existence_ok: bool = False,
    ) -> tuple[str, int]:
        """
        Executes the input query and add the feature to the feature registry and the version registry

        Args:
            feature_name (str): name of the feature
            feature_desc (str): description of the feature
            feature_format (str): format of the feature e.g. one-hot, binary, continuous
            table_name (str): table_name of the feature, str
            sql_select_query_to_generate_feature (str): SQL query that generates the feature
            existence_ok (bool): if True, the function will not raise an error if the feature already exists 
                (default: False)
        
        Returns:
            tuple[str, int]: feature_id and feature_version
        """
        session = self.conn.session

        # Add the feature to the feature registry
        self.conn.use_schema(self.metadata_schema)
        existing_feature_names = [r['FEATURE_NAME'] for r in 
                    session.sql(f"""
                    select feature_name 
                    from feature_registry;""").collect()]
        # print(existing_feature_names)

        feature_name = feature_name.upper()
        feature_name = feature_name.strip()
        feature_name = feature_name.replace(" ", "_")

        if feature_name in existing_feature_names and existence_ok == False:
            raise ValueError(f"""Feature {feature_name} already exists. Please check you are not creating a duplicate 
                             and try again with a different name.""")
        
        if feature_name not in existing_feature_names:
            session.sql(f"""
                INSERT INTO feature_registry (
                        feature_name, 
                        feature_desc, 
                        feature_format, 
                        date_feature_registered)
                VALUES ('{feature_name}', '{feature_desc}', '{feature_format}', CURRENT_TIMESTAMP)
            """).collect()  # vulnerable to SQL injection - never expose externally

        feature_id = session.sql(f"""SELECT MAX(feature_id)
                                FROM feature_registry
                                WHERE feature_name = '{feature_name}'""").collect()[0][
                                "MAX(FEATURE_ID)"]
        self.conn.use_schema(self.schema)

        if feature_name in existing_feature_names and existence_ok == True: # by definition, existence_ok == True since already raised an error if False
            return feature_id, self._get_current_feature_version(feature_id)[0]

        if feature_name not in existing_feature_names:
            print(
                f"Feature {feature_name}, ID {feature_id}, added to the feature registry; table not created yet"
            )

        try:
            table_name = self._create_feature_table(
                feature_name, feature_id, sql_select_query_to_generate_feature,
                comment = feature_desc
            )
            print(sql_select_query_to_generate_feature)
            feature_version = self._add_new_feature_version_to_version_registry(
                feature_id,
                table_name,
                sql_select_query_to_generate_feature,
                "Initial version",
            )

        except Exception as e:
            print(f"Error creating table: {e}. Registry entry will now be deleted")
            self.conn.use_schema(self.metadata_schema)
            session.sql(f"""
                DELETE FROM feature_registry
                WHERE feature_id = '{feature_id}'
            """).collect()
            self.conn.use_schema(self.schema)
            raise e

        return feature_id, feature_version

    def _check_table_exists(self, table_name: str, schema: str = None):
        if schema is None:
            schema = self.schema
        session = self.conn.session
        existing_tables = [
            r.as_dict()["TABLE_NAME"]
            for r in session.sql(f"""SELECT TABLE_NAME
                                    FROM INFORMATION_SCHEMA.TABLES
                                    WHERE TABLE_CATALOG = '{self.database}'
                                    AND TABLE_SCHEMA = '{schema}';""").collect()
        ]  # returns empty list if no tables
        if table_name.upper() in existing_tables:
            raise ValueError(f"{self.database}.{schema}.{table_name} already exists.")

    def _create_table_creation_query_from_select_query(
        self,
        table_name: str,
        select_query: str,
    ) -> str:
        
        full_query = f"""
                CREATE TABLE {table_name.upper()}
                AS {select_query}
                """
        return full_query

    def _get_feature_table_name(self, feature_name: str, feature_id: str) -> str:
        feature_version = self._get_current_feature_version(feature_id)[1]
        return self._table_naming_convention(feature_name, feature_version)

    def _table_naming_convention(self, feature_name: str, feature_version: int) -> str:
        return f"{feature_name}_v{feature_version}"

    def _create_feature_table(
        self, feature_name: str, feature_id: str, select_query: str, comment: str
    ):
        session = self.conn.session
        table_name = self._get_feature_table_name(feature_name, feature_id)

        # Quality checks
        self._check_table_exists(table_name)
        if table_name.upper() in self.table_names:
            raise ValueError(f"{table_name.upper()} is a reserved name.")

        # Generate the full sql query
        full_query = self._create_table_creation_query_from_select_query(
            table_name, select_query
        )

        # Create the table
        session.sql(full_query).collect()

        session.sql(f"""COMMENT ON TABLE {table_name} IS '{comment}'""").collect()

        print(f"Table {table_name} created successfully")

        return table_name

    def _get_current_feature_version(self, feature_id: str) -> tuple[int, int]:
        """
        Get the current feature version and the next feature version for a given feature_id
        Args:
            feature_id (str): the feature_id of the feature
        Returns:
            tuple[int, int]: the current feature version and the next feature version
        """
        session = self.conn.session

        self.conn.use_schema(self.metadata_schema)
        result = session.sql(f"""
                SELECT COALESCE(MAX(feature_version), 0) AS max_version
                FROM feature_version_registry
                WHERE feature_id = '{feature_id}'
                """).collect()
        self.conn.use_schema(self.schema)
        current_version = result[0]["MAX_VERSION"]
        next_version = current_version + 1

        return current_version, next_version

    def _add_new_feature_version_to_version_registry(
        self, feature_id: str, table_name: str, sql_query: str, change_description: str
    ) -> int:
        session = self.conn.session

        feature_version = self._get_current_feature_version(feature_id)[1]

        escaped_sql_query = sql_query.replace("'", "''")
        self.conn.use_schema(self.metadata_schema)
        session.sql(f"""INSERT INTO feature_version_registry (
                        feature_ID,
                        feature_version,
                        table_name,
                        sql_query,
                        change_description,
                        date_version_registered)
                VALUES (
                    '{feature_id}',
                    {feature_version},
                    '{table_name.upper()}',
                    '{escaped_sql_query}',
                    '{change_description}',
                    CURRENT_TIMESTAMP)
            """).collect()
        self.conn.use_schema(self.schema)

        print(
            f"Feature {feature_id} version {feature_version} added to the feature version registry"
        )

        return feature_version

    def update_feature(
        self,
        feature_id: str,
        new_sql_select_query: str,
        change_description: str,
        force_new_version: bool = False,
    ) -> int:
        """
        Updates the feature in the feature version registry with the new query and description

        Args:
            feature_id (str): uuid of the feature
            new_sql_select_query (str): new SQL query that generates the feature
            change_description (str): description of the change
            force_new_version (bool): if True, the function will create a new version even if the query is the same
                (default: False)

        Returns:
            int: the new feature version
        """
        session = self.conn.session

        self.conn.use_schema(self.metadata_schema)
        if not session.sql(
            f"""SELECT feature_id FROM feature_registry WHERE feature_id = '{feature_id}'"""
        ).collect():
            raise ValueError(f"Feature {feature_id} does not exist.")
        else:
            feature_name = session.sql(
                f"""SELECT feature_name FROM feature_registry WHERE feature_id = '{feature_id}'"""
            ).collect()[0]["FEATURE_NAME"]
            feature_desc = session.sql(
                f"""SELECT feature_desc FROM feature_registry WHERE feature_id = '{feature_id}'"""
            ).collect()[0]["FEATURE_DESC"]
            sql_queries_raw = session.sql(f"""SELECT sql_query FROM feature_version_registry 
                WHERE feature_id = '{feature_id}'""").collect()
            sql_queries = [r["SQL_QUERY"] for r in sql_queries_raw]
            if new_sql_select_query in sql_queries:
                # was having problems where, if a notebook cell was run twice, a new version was created, so now
                # defaulte behaviour is that the query must be different to all previous; can be forced with force_new_version
                if not force_new_version:
                    raise ValueError(
                        f"Feature {feature_id} has not been updated. The new query is the same as the last one. Use" /
                        "force_new_version=True to force a new version."
                    )
            print(f"Updating feature {feature_name}, with ID {feature_id}")
        self.conn.use_schema(self.schema)

        # Create new feature and add to registry
        table_name = self._create_feature_table(
            feature_name, feature_id, new_sql_select_query, feature_desc + change_description
        )
        feature_version = self._add_new_feature_version_to_version_registry(
            feature_id, table_name, new_sql_select_query, change_description
        )

        # Stop live updating on the old version of the feature
        # old_version = feature_version - 1
        # old_table_name = session.sql(f"""
        #                          SELECT table_name FROM feature_version_registry 
        #                          WHERE feature_id = '{feature_id}' AND feature_version = {old_version}""").collect()[
        #     0
        # ]["TABLE_NAME"]
        # session.sql(
        #     f"""ALTER DYNAMIC TABLE {old_table_name} SUSPEND;"""
        # ).collect()  # existing data still exists
        # print(
        #     f"Table {old_table_name.upper()} has been suspended: data exists but no new data will be added"
        # )
        # # note dynamic tables can be resumed if wanted
        # session.sql(
        #     f"""UPDATE feature_version_registry SET live_updating = FALSE 
        #     WHERE feature_id = '{feature_id}' AND feature_version = {old_version}"""
        # ).collect()

        return feature_version

    # def deprecate_feature(self, feature_id: int):
    #     """
    #     Deprecate a feature by setting the live_updating flag to False
    #     """
    #     session = self.conn.session
    #     table_names = [r["TABLE_NAME"] for r in session.sql(
    #         f"""SELECT table_name
    #         FROM feature_version_registry 
    #         WHERE feature_id = '{feature_id}'
    #         AND live_updating = TRUE;"""
    #     ).collect()]
    #     print(f"The following tables are live: {table_names}")

    #     session.sql(
    #         f"""UPDATE feature_version_registry 
    #         SET live_updating = FALSE 
    #         WHERE feature_id = '{feature_id}';"""
    #     ).collect()

    #     for table_name in table_names:
    #         session.sql(
    #             f"""alter dynamic table {table_name} suspend;"""
    #         ).collect()  # existing table frozen
    #     print(f"Feature {feature_id} has been deprecated")
    
    def get_latest_feature_version(self, feature_id: str) -> int:
        """
        Get the latest version of a feature from the feature id
        
        Args:
            feature_id (str): the feature_id of the feature
        
        Returns:
            int: the latest version of the feature
        """
        session = self.conn.session
        self.conn.use_schema(self.metadata_schema)
        latest_version = session.sql(
            f"""SELECT COALESCE(MAX(feature_version), 0) AS max_version
            FROM feature_version_registry
            WHERE feature_id = '{feature_id}'"""
        ).collect()[0]["MAX_VERSION"]
        self.conn.use_schema(self.schema)
        return latest_version
    
    def get_feature_id_from_table_name(self, table_name: str) -> str:
        """
        Get the feature_id from the table_name
        
        Args:
            table_name (str): the table_name of the feature
            
        Returns:
            str: the feature_id (uuid) of the feature
        """
        session = self.conn.session
        self.conn.use_schema(self.metadata_schema)
        result = session.sql(
            f"""SELECT feature_id
            FROM feature_version_registry
            WHERE table_name = '{table_name.upper()}'"""
        ).collect()
        self.conn.use_schema(self.schema)
        if result:
            return result[0]["FEATURE_ID"]
        else:
            return []
    
    def delete_feature(self, feature_id: str):
        """
        Delete a feature i.e. all data tables and all entries in the feature registry and version registry
        This is intended to be used only when features are created in error and not for retiring features that have been
        used in models. 

        Args:
            feature_id (str): the feature_id of the feature
        """
        #  Get the names of the relevant tables
        session = self.conn.session
        self.conn.use_schema(self.metadata_schema)
        all_tables = session.sql(
            f"""SELECT table_name
            FROM feature_version_registry
            WHERE feature_id = '{feature_id}'"""
        ).collect()
        print(all_tables)

        # Delete the relevant tables
        self.conn.use_schema(self.schema)
        for table in all_tables:
            table_name = table["TABLE_NAME"]
            print(f"Deleting table {table_name}")
            session.sql(f"""DROP TABLE IF EXISTS {table_name}""").collect()
            print(f"Table {table_name} deleted successfully")

        # Delete records from version registry
        self.conn.use_schema(self.metadata_schema)
        session.sql(f"DELETE FROM feature_version_registry WHERE feature_id = '{feature_id}'").collect()

        # Delete from feature registry
        session.sql(f"DELETE FROM feature_registry WHERE feature_id = '{feature_id}'").collect()

    # TODO: feature version deleter

if __name__ == "__main__":
    load_dotenv()
    conn = SnowflakeConnection()
    DATABASE = "INTELLIGENCE_DEV"
    SCHEMA = "TEST_FEATURE_STORE_IW_2"
    feature_store_manager = FeatureStoreManager(conn, DATABASE, SCHEMA)
    # feature_store_manager.create_feature_store()
    # feature_store_manager.deprecate_feature(1)
    print(feature_store_manager.get_latest_feature_version(1))

    # feature_store_manager.add_new_feature(
    #     "hypertension",
    #     "patients with a high BP phenotype",
    #     "continuous",
    #     "SELECT * FROM INTELLIGENCE_DEV.AI_CENTRE_DEV.BSA_BNF_LOOKUP",
    # )
    # feature_store_manager.update_feature(1, "SELECT * FROM INTELLIGENCE_DEV.AI_CENTRE_DEV.BSA_BNF_LOOKUP LIMIT 5", "test update")
    # feature_version = feature_store_manager.update_feature(1,
    #                                                        "SELECT * FROM INTELLIGENCE_DEV.AI_CENTRE_DEV.BSA_BNF_LOOKUP LIMIT 3",
    #                                                        "another test update")
    # print(feature_version)

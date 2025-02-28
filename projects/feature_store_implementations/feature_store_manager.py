
from dotenv import load_dotenv

from phmlondon.snow_utils import SnowflakeConnection

# logging.basicConfig(level=logging.DEBUG) # optional, for debugging

load_dotenv()
conn = SnowflakeConnection()


class FeatureStoreManager:
    def __init__(self, connection: SnowflakeConnection, database: str, schema: str):
        """
        The schema where you want to create the feature store should already exist
        """
        self.conn = connection
        self.database = database
        self.schema = schema
        self.conn.use_database(self.database)
        self.conn.use_schema(self.schema)
        self.table_names = ["FEATURE_REGISTRY", "FEATURE_VERSION_REGISTRY", "ACTIVE_FEATURES"]

    def create_feature_store(self):
        """
        Creates the FEATURE_REGISTRY if it doesn't exist.
        Raises an Exception if the table already exists.
        """
        session = self.conn.session

        table_names = self.table_names

        self._check_table_exists(table_names[0])
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_names[0]} (
                    feature_id INT PRIMARY KEY AUTOINCREMENT START 1 INCREMENT 1 ORDER,
                    feature_name VARCHAR NOT NULL,
                    feature_desc VARCHAR,
                    feature_format VARCHAR,
                    table_name VARCHAR,
                    date_feature_registered TIMESTAMP
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
                lag VARCHAR,
                change_description VARCHAR,
                date_version_registered TIMESTAMP
            ) """).collect()
        print(f"{table_names[1]} created successfully.")

        self._check_table_exists(table_names[2])
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_names[2]} (
                    feature_id INT,
                    feature_version INT,
                    model_id INT,
                    model_version INT,
                    date_registered_as_active TIMESTAMP
                )  """).collect()  # every active model should register a new entry, even if using the same feature
        print(f"{table_names[2]} created successfully.")

    def add_new_feature(
        self,
        feature_name: str,
        feature_desc: str,
        feature_format: str,
        sql_select_query_to_generate_feature: str,
        target_lag: str = "1 day",
    ) -> tuple[int, int]:
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
        """).collect()  # vulnerable to SQL injection - never expose externally
        feature_id = session.sql(f"""SELECT MAX(feature_id)
                                  FROM feature_registry
                                  WHERE feature_name = '{feature_name}'""").collect()[0][
            "MAX(FEATURE_ID)"
        ]
        print(
            f"Feature {feature_name}, ID {feature_id}, added to the feature registry; table not created yet"
        )

        try:
            table_name = self._create_feature_table(
                feature_name, feature_id, target_lag, sql_select_query_to_generate_feature
            )
            print(sql_select_query_to_generate_feature)
            feature_version = self._add_new_feature_version_to_version_registry(
                feature_id,
                table_name,
                sql_select_query_to_generate_feature,
                target_lag,
                "Initial version",
            )

        except Exception as e:
            print(f"Error creating table: {e}. Registry entry will now be deleted")
            session.sql(f"""
                DELETE FROM feature_registry
                WHERE feature_id = {feature_id}
            """).collect()
            raise e

        return feature_id, feature_version

    def _check_table_exists(self, table_name: str):
        session = self.conn.session
        existing_tables = [
            r.as_dict()["TABLE_NAME"]
            for r in session.sql(f"""SELECT TABLE_NAME
                                                                                FROM INFORMATION_SCHEMA.TABLES
                                                                                WHERE TABLE_CATALOG = '{self.database}'
                                                                                AND TABLE_SCHEMA = '{self.schema}';""").collect()
        ]  # returns empty list if no tables
        if table_name in existing_tables:
            raise ValueError(f"{self.database}.{self.schema}.{table_name} already exists.")

    def _create_table_creation_query_from_select_query(
        self, table_name: str, target_lag: str, select_query: str
    ) -> str:
        full_query = f"""
                CREATE DYNAMIC TABLE {table_name}
                TARGET_LAG = '{target_lag}'
                WAREHOUSE = INTELLIGENCE_XS
                REFRESH_MODE = INCREMENTAL
                INITIALIZE = ON_CREATE
                AS
                {select_query}
            """
        return full_query

    def _get_feature_table_name(self, feature_name: str, feature_id: int) -> str:
        feature_version = self._get_current_feature_version(feature_id)[1]
        return self._table_naming_convention(feature_name, feature_version)

    def _table_naming_convention(self, feature_name: str, feature_version: int) -> str:
        return f"{feature_name}_v{feature_version}"

    def _create_feature_table(
        self, feature_name: str, feature_id: int, target_lag: str, select_query: str
    ):
        session = self.conn.session
        table_name = self._get_feature_table_name(feature_name, feature_id)

        # Quality checks
        self._check_table_exists(table_name)
        if table_name in self.table_names:
            raise ValueError(f"{table_name} is a reserved name.")

        # Generate the full sql query
        full_query = self._create_table_creation_query_from_select_query(
            table_name, target_lag, select_query
        )

        # Create the table
        session.sql(full_query).collect()

        print(f"Table {table_name} created successfully")

        return table_name

    def _get_current_feature_version(self, feature_id: int) -> tuple[int, int]:
        session = self.conn.session

        result = session.sql(f"""
                SELECT COALESCE(MAX(feature_version), 0) AS max_version
                FROM feature_version_registry
                WHERE feature_id = {feature_id}
                """).collect()
        current_version = result[0]["MAX_VERSION"]
        next_version = current_version + 1

        return current_version, next_version

    def _add_new_feature_version_to_version_registry(
        self, feature_id: int, table_name: str, sql_query: str, lag: str, change_description: str
    ) -> int:
        session = self.conn.session

        feature_version = self._get_current_feature_version(feature_id)[1]

        live_updating = True  # hardcode that whenevr a new feature is added, it is live updating

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
                    {feature_id},
                    {feature_version},
                    '{table_name}',
                    '{sql_query}',
                    {live_updating},
                    '{lag}',
                    '{change_description}',
                    CURRENT_TIMESTAMP)
            """).collect()

        print(
            f"Feature {feature_id} version {feature_version} added to the feature version registry"
        )

        return feature_version

    def update_feature(
        self,
        feature_id: int,
        new_sql_select_query: str,
        change_description: str,
        target_lag: str = "",
    ) -> int:
        """
        Updates the feature in the feature version registry with the new query and description
        feature_id: id of the feature, int
        new_sql_select_query: new SQL query that generates the feature, str
        change_description: description of the change, str
        """
        session = self.conn.session

        if not session.sql(
            f"""SELECT feature_id FROM feature_registry WHERE feature_id = {feature_id}"""
        ).collect():
            raise ValueError(f"Feature {feature_id} does not exist.")
        else:
            feature_name = session.sql(
                f"""SELECT feature_name FROM feature_registry WHERE feature_id = {feature_id}"""
            ).collect()[0]["FEATURE_NAME"]
            print(f"Updating feature {feature_name}, with ID {feature_id}")

        if not target_lag:
            # if no target lag given to the update function, use the same lag as the most recent
            target_lag = session.sql(f"""
                SELECT lag
                FROM feature_version_registry
                WHERE feature_id = {feature_id}
                ORDER BY date_version_registered DESC
                LIMIT 1
            """).collect()[0]["LAG"]

        # Create new feature and add to registry
        table_name = self._create_feature_table(
            feature_name, feature_id, target_lag, new_sql_select_query
        )
        feature_version = self._add_new_feature_version_to_version_registry(
            feature_id, table_name, new_sql_select_query, target_lag, change_description
        )

        # Stop live updating on the old version of the feature
        old_version = feature_version - 1
        old_table_name = session.sql(f"""
                                 SELECT table_name FROM feature_version_registry 
                                 WHERE feature_id = {feature_id} AND feature_version = {old_version}""").collect()[
            0
        ]["TABLE_NAME"]
        session.sql(
            f"""ALTER DYNAMIC TABLE {old_table_name} SUSPEND;"""
        ).collect()  # existing data still exists
        print(
            f"Table {old_table_name.upper()} has been suspended: data exists but no new data will be added"
        )
        # note dynamic tables can be resumed if wanted
        session.sql(
            f"""UPDATE feature_version_registry SET live_updating = FALSE WHERE feature_id = {feature_id} AND feature_version = {old_version}"""
        ).collect()

        return feature_version

    # def get_data

    def deprecate_feature(self, feature_id: int):
        """
        Deprecate a feature by setting the live_updating flag to False
        """
        session = self.conn.session
        table_names = [r["TABLE_NAME"] for r in session.sql(
            f"""SELECT table_name
            FROM feature_version_registry 
            WHERE feature_id = {feature_id}
            AND live_updating = TRUE;"""
        ).collect()]
        print(f"The following tables are live: {table_names}")

        session.sql(
            f"""UPDATE feature_version_registry 
            SET live_updating = FALSE 
            WHERE feature_id = {feature_id};"""
        ).collect()

        for table_name in table_names:
            session.sql(
                f"""alter dynamic table {table_name} suspend;"""
            ).collect()  # existing table frozen
        print(f"Feature {feature_id} has been deprecated")
    
    def get_latest_feature_version(self, feature_id: int) -> int:
        session = self.conn.session
        latest_version = session.sql(
            f"""SELECT COALESCE(MAX(feature_version), 0) AS max_version
            FROM feature_version_registry
            WHERE feature_id = {feature_id}"""
        ).collect()[0]["MAX_VERSION"]
        return latest_version

if __name__ == "__main__":
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

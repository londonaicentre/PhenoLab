from flask import Flask
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

app = Flask(__name__)

@app.route("/")
def home():
    # conn = SnowflakeConnection()
    load_dotenv()
            
    connection_parameters = {
            "account": os.getenv("SNOWFLAKE_SERVER"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "role": os.getenv("SNOWFLAKE_USERGROUP"),
            "authenticator": "externalbrowser",
        }

    session = Session.builder.configs(connection_parameters).create()

    results = session.sql("SELECT * FROM PROD_DWH.ANALYST_PRIMARY_CARE.CONCEPT LIMIT 10").collect()
    return results

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

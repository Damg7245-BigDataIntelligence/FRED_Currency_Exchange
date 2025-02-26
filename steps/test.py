from snowflake.snowpark import Session
from dotenv import load_dotenv
import os

load_dotenv()

snowflake_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

try:
    session = Session.builder.configs(snowflake_params).create()
    print("✅ Snowflake connection successful!")
    session.close()
except Exception as e:
    print("❌ Snowflake connection failed:", e)

from snowflake.snowpark.functions import udf
from snowflake.snowpark.types import FloatType, StringType
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Snowflake connection parameters
snowflake_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": "FRED_DB",
    "schema": "HARMONIZED",  # Specify the functions schema
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

# Define a Python function for forward filling
def forward_fill(value, last_valid):
    if value is not None and value != '.':
        return float(value)
    return last_valid

def main():
    # Connect to Snowflake
    try:
        session = Session.builder.configs(snowflake_params).create()
        print("✅ Snowflake connection successful!")
    except Exception as e:
        print("❌ Snowflake connection failed:", e)
        return

    # Set the schema
    session.sql("USE SCHEMA HARMONIZED").collect()

    # Create a stage if it doesn't exist
    session.sql("CREATE STAGE IF NOT EXISTS my_stage").collect()

    # Register the UDF
    session.udf.register(
        forward_fill,
        return_type=FloatType(),
        input_types=[StringType(), FloatType()],
        name="FORWARD_FILL_UDF",
        is_permanent=True,
        replace=True,
        stage_location="@my_stage"  # Ensure this stage exists in Snowflake
    )

    # Close session
    session.close()
    print("🔄 Snowflake session closed.")

if __name__ == "__main__":
    main()
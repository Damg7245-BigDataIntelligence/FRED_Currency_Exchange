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
    "schema": "EXTERNAL",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

STAGES = ["DAILYCURRENCY_RAW_STAGE", "MONTHLYCURRENCYEXCHANGE_RAW_STAGE"]
RAW_SCHEMA = "RAW_SCHEMA"

# Connect to Snowflake
try:
    session = Session.builder.configs(snowflake_params).create()
    print("✅ Snowflake connection successful!")
except Exception as e:
    print("❌ Snowflake connection failed:", e)
    exit()

# Iterate over stages
for stage in STAGES:
    print(f"🔍 Checking files in stage: {stage}")

    # Get list of CSV files from the stage
    list_files_sql = f"LIST @{stage};"
    files = session.sql(list_files_sql).collect()

    if stage.startswith("DAILY"):
        schema_name = "RAW_DAILY"
    elif stage.startswith("MONTHLY"):
        schema_name = "RAW_MONTHLY"
    
    for file in files:
        file_path = file[0]  # Full path
        file_name = os.path.basename(file_path).split('.')[0]  # Extract filename without extension
        table_name = f"RAW_{file_name.upper()}"  # Create table name

        print(f"📂 Processing file: {file_name}, creating table: {table_name}")

        # Create table dynamically with inferred schema
        create_table_sql = f"""
        CREATE OR REPLACE TABLE {schema_name}.{table_name}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) 
            FROM TABLE(INFER_SCHEMA(
                location => '@{stage}/{file_name}.csv',
                FILE_FORMAT => 'CSV_FORMAT'
            ))
        );
        """
        session.sql(create_table_sql).collect()
        print(f"✅ Created table: {table_name}")

        # Copy data into the table
        copy_into_sql = f"""
        COPY INTO {schema_name}.{table_name}
        FROM @{stage}/{file_name}.csv
        FILE_FORMAT = CSV_FORMAT
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

        """
        session.sql(copy_into_sql).collect()
        print(f"✅ Data loaded into {table_name}")

# Close session
session.close()
print("🔄 Snowflake session closed.")

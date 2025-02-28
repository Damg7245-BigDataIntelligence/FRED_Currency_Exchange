from snowflake.snowpark import Session
from snowflake.snowpark import functions as F
from dotenv import load_dotenv
import os
from snowflake.snowpark import window


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

raw_data = {
    "RAW_DAILY": ["RAW_DEXINUS", "RAW_DEXUSEU", "RAW_DEXUSUK"],
    "RAW_MONTHLY": ["RAW_EXINUS", "RAW_EXUSEU", "RAW_EXUSUK"]
}

def create_harmonized_view(session, raw_data):
    session.use_schema("HARMONIZED")

    for schema_name, tables in raw_data.items():
        base_df = None  # Initialize base DataFrame

        for table in tables:
            table_suffix = table.replace("RAW_", "")  # Extract suffix (e.g., exinus)
            df = session.table(f"{schema_name}.{table}").select(F.col('"date"'), F.col('"value"').alias(table_suffix))

            # Fill missing dates
            #df = fill_missing_dates(session, df)

            # Merge on "date" column
            base_df = df if base_df is None else base_df.join(df, on='"date"', how="outer")

        # Create view for daily and monthly separately
        view_name = f"HARMONIZED_{schema_name.split('_')[1]}_V".upper()
        base_df.create_or_replace_view(view_name)
        print(f"✅ {view_name} created successfully!")


try:
    session = Session.builder.configs(snowflake_params).create()
    print("✅ Snowflake connection successful!")
    create_harmonized_view(session, raw_data)
    session.close()
    print("🔄 Snowflake session closed.")
except Exception as e:
    print("❌ Snowflake connection failed:", e)
    exit()

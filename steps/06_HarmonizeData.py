from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when, max as max_, udf
import pandas as pd
from snowflake.snowpark.types import T
from dotenv import load_dotenv
import os

load_dotenv()

snowflake_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": "FRED_DB",
    "schema": "EXTERNAL",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}



# Define a Python function for forward filling
def forward_fill(values):
    filled_values = []
    last_valid = None
    for value in values:
        if value is not None and value != '.':
            last_valid = float(value)
        filled_values.append(last_valid)
    return filled_values

# Register the UDF with Snowflake
forward_fill_udf = udf(forward_fill, return_type=T.ArrayType(T.FloatType()), input_types=[T.ArrayType(T.StringType())])


def harmonize_currency_data(session):
    """
    Harmonizes daily and monthly currency exchange data into a single table.
    """
    # Load daily and monthly data for INRUSD, EURUSD, GBPUSD
    inrusd = session.table("RAW_DAILY.DEXINUS").select("date", "value").unionAll(
        session.table("RAW_MONTHLY.EXINUS").select("date", "value")
    )
    
    eurusd = session.table("RAW_DAILY.DEXUSEU").select("date", "euro_to_usd").unionAll(
        session.table("RAW_MONTHLY.EXUSEU").select("date", "euro_to_usd")
    )
    
    gbpusd = session.table("RAW_DAILY.DEXUSUK").select("date", "pound_to_usd").unionAll(
        session.table("RAW_MONTHLY.EXUSUK").select("date", "pound_to_usd")
    )

    # Apply the UDF to handle missing values
    inrusd = inrusd.groupBy("date").agg(
        forward_fill_udf(col("value")).alias("value")
    )

    eurusd = eurusd.groupBy("date").agg(
        forward_fill_udf(col("euro_to_usd")).alias("euro_to_usd")
    )

    gbpusd = gbpusd.groupBy("date").agg(
        forward_fill_udf(col("pound_to_usd")).alias("pound_to_usd")
    )

    # Create the harmonized table
    session.sql("CREATE OR REPLACE TABLE HARMONIZED_CURRENCY_EXCHANGE (date STRING, currency_pair STRING, value FLOAT)")

    # Insert harmonized data into the table
    inrusd.withColumn("currency_pair", col("INRUSD")).write.mode("append").saveAsTable("HARMONIZED_CURRENCY_EXCHANGE")
    eurusd.withColumn("currency_pair", col("EURUSD")).write.mode("append").saveAsTable("HARMONIZED_CURRENCY_EXCHANGE")
    gbpusd.withColumn("currency_pair", col("GBPUSD")).write.mode("append").saveAsTable("HARMONIZED_CURRENCY_EXCHANGE")

    print("✅ Harmonized currency data created successfully!")
    
# Connect to Snowflake
try:
    session = Session.builder.configs(snowflake_params).create()
    print("Snowflake connection successful!")
except Exception as e:
    print("Snowflake connection failed:", e)
    exit()

harmonize_currency_data(session)
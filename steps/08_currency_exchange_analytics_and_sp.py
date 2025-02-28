import os
from snowflake.snowpark import Session
from dotenv import load_dotenv
import snowflake.snowpark.functions as F

# Load environment variables
load_dotenv()

# Snowflake connection parameters
snowflake_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": "FRED_DB",
    "schema": "ANALYTICS",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

def create_analytics_tables(session):
    # Create a table for daily data metrics
    session.sql("""
        CREATE OR REPLACE TABLE DAILY_DATA_METRICS AS
        SELECT
            date,
            usdtoinr,
            usdtoeuro,
            usdto_pound,
            NULL AS rate_change_percent_usdtoinr,
            NULL AS rate_change_percent_usdtoeuro,
            NULL AS rate_change_percent_usdto_pound,
            NULL AS volatility_usdtoinr,
            NULL AS volatility_usdtoeuro,
            NULL AS volatility_usdto_pound
        FROM HARMONIZED.DAILY_TABLE
    """).collect()

    # Create a table for monthly data metrics
    session.sql("""
        CREATE OR REPLACE TABLE MONTHLY_DATA_METRICS AS
        SELECT
            date,
            usdtoinr,
            usdtoeuro,
            usdto_pound,
            NULL AS rate_change_percent_usdtoinr,
            NULL AS rate_change_percent_usdtoeuro,
            NULL AS rate_change_percent_usdto_pound,
            NULL AS volatility_usdtoinr,
            NULL AS volatility_usdtoeuro,
            NULL AS volatility_usdto_pound
        FROM HARMONIZED.MONTHLY_TABLE
    """).collect()

def create_stored_procedure(session):
    # Define the stored procedure for updating data metrics
    session.sql("""
        CREATE OR REPLACE PROCEDURE UPDATE_DATA_METRICS()
        RETURNS STRING
        LANGUAGE SQL
        AS
        $$
        BEGIN
            -- Update daily metrics for each currency pair
            UPDATE ANALYTICS.DAILY_DATA_METRICS
            SET
                rate_change_percent_usdtoinr = CASE WHEN LAG(usdtoinr) OVER (ORDER BY date) IS NULL THEN NULL ELSE (LAG(usdtoinr) OVER (ORDER BY date) - usdtoinr) / NULLIF(usdtoinr, 0) * 100 END,
                rate_change_percent_usdtoeuro = CASE WHEN LAG(usdtoeuro) OVER (ORDER BY date) IS NULL THEN NULL ELSE (LAG(usdtoeuro) OVER (ORDER BY date) - usdtoeuro) / NULLIF(usdtoeuro, 0) * 100 END,
                rate_change_percent_usdto_pound = CASE WHEN LAG(usdto_pound) OVER (ORDER BY date) IS NULL THEN NULL ELSE (LAG(usdto_pound) OVER (ORDER BY date) - usdto_pound) / NULLIF(usdto_pound, 0) * 100 END,
                volatility_usdtoinr = STDDEV(usdtoinr) OVER (ORDER BY date),
                volatility_usdtoeuro = STDDEV(usdtoeuro) OVER (ORDER BY date),
                volatility_usdto_pound = STDDEV(usdto_pound) OVER (ORDER BY date);

            -- Update monthly metrics for each currency pair
            UPDATE ANALYTICS.MONTHLY_DATA_METRICS
            SET
                rate_change_percent_usdtoinr = CASE WHEN LAG(usdtoinr) OVER (ORDER BY date) IS NULL THEN NULL ELSE (LAG(usdtoinr) OVER (ORDER BY date) - usdtoinr) / NULLIF(usdtoinr, 0) * 100 END,
                rate_change_percent_usdtoeuro = CASE WHEN LAG(usdtoeuro) OVER (ORDER BY date) IS NULL THEN NULL ELSE (LAG(usdtoeuro) OVER (ORDER BY date) - usdtoeuro) / NULLIF(usdtoeuro, 0) * 100 END,
                rate_change_percent_usdto_pound = CASE WHEN LAG(usdto_pound) OVER (ORDER BY date) IS NULL THEN NULL ELSE (LAG(usdto_pound) OVER (ORDER BY date) - usdto_pound) / NULLIF(usdto_pound, 0) * 100 END,
                volatility_usdtoinr = STDDEV(usdtoinr) OVER (ORDER BY date),
                volatility_usdtoeuro = STDDEV(usdtoeuro) OVER (ORDER BY date),
                volatility_usdto_pound = STDDEV(usdto_pound) OVER (ORDER BY date);

            RETURN 'Data metrics updated successfully';
        END;
        $$;
    """).collect()

def main():
    # Connect to Snowflake
    try:
        session = Session.builder.configs(snowflake_params).create()
        print("✅ Snowflake connection successful!")
    except Exception as e:
        print("❌ Snowflake connection failed:", e)
        return

    # Create analytics tables
    create_analytics_tables(session)

    # Create the stored procedure
    create_stored_procedure(session)

    # Optionally, call the stored procedure to update analytics
    session.sql("CALL UPDATE_DATA_METRICS()").collect()

    # Close session
    session.close()
    print("🔄 Snowflake session closed.")

if __name__ == "__main__":
    main()
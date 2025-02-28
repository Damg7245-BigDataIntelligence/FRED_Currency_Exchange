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
    # Set the schema to HARMONIZED and verify the source tables exist
    session.sql("USE SCHEMA HARMONIZED").collect()
    tables = session.sql("SHOW TABLES LIKE 'HARMONIZED_DAILY_TBL'").collect()
    if not tables:
        raise Exception("Table HARMONIZED_DAILY_TBL does not exist.")
    tables = session.sql("SHOW TABLES LIKE 'HARMONIZED_MONTHLY_TBL'").collect()
    if not tables:
        raise Exception("Table HARMONIZED_MONTHLY_TBL does not exist.")
    
    # Switch to ANALYTICS schema and create the metrics tables
    session.sql("USE SCHEMA ANALYTICS").collect()
    
    # Create the daily metrics table
    session.sql("""
        CREATE OR REPLACE TABLE DAILY_DATA_METRICS AS
        SELECT
            DDATE,
            DEXINUS,
            DEXUSEU_CONVERTED,
            DEXUSUK_CONVERTED,
            NULL AS rate_change_percent_dexinus,
            NULL AS rate_change_percent_dexuseu_converted,
            NULL AS rate_change_percent_dexusuk_converted,
            NULL AS volatility_dexinus,
            NULL AS volatility_dexuseu_converted,
            NULL AS volatility_dexusuk_converted
        FROM HARMONIZED.HARMONIZED_DAILY_TBL
    """).collect()
    
    # Create the monthly metrics table
    session.sql("""
        CREATE OR REPLACE TABLE MONTHLY_DATA_METRICS AS
        SELECT
            MDATE,
            EXUSEU_CONVERTED,
            EXUSUK_CONVERTED,
            NULL AS rate_change_percent_exuseu_converted,
            NULL AS rate_change_percent_exusuk_converted,
            NULL AS volatility_exuseu_converted,
            NULL AS volatility_exusuk_converted
        FROM HARMONIZED.HARMONIZED_MONTHLY_TBL
    """).collect()

def create_stored_procedure(session):
    session.sql("""
        CREATE OR REPLACE PROCEDURE UPDATE_DATA_METRICS()
        RETURNS STRING
        LANGUAGE SQL
        AS
        $$
        BEGIN
          -- Create a temporary table with computed daily metrics
          CREATE OR REPLACE TEMPORARY TABLE TMP_UPDATED_DAILY_METRICS AS
          WITH daily_metrics AS (
            SELECT
              DDATE,
              DEXINUS,
              DEXUSEU_CONVERTED,
              DEXUSUK_CONVERTED,
              LAG(DEXINUS) OVER (ORDER BY DDATE) AS prev_dexinus,
              LAG(DEXUSEU_CONVERTED) OVER (ORDER BY DDATE) AS prev_dexuseu_converted,
              LAG(DEXUSUK_CONVERTED) OVER (ORDER BY DDATE) AS prev_dexusuk_converted,
              STDDEV(DEXINUS) OVER (ORDER BY DDATE) AS volatility_dexinus,
              STDDEV(DEXUSEU_CONVERTED) OVER (ORDER BY DDATE) AS volatility_dexuseu_converted,
              STDDEV(DEXUSUK_CONVERTED) OVER (ORDER BY DDATE) AS volatility_dexusuk_converted
            FROM HARMONIZED.HARMONIZED_DAILY_TBL
          )
          SELECT
            DDATE,
            CASE WHEN prev_dexinus IS NULL THEN NULL ELSE (DEXINUS - prev_dexinus) / NULLIF(prev_dexinus, 0) * 100 END AS rate_change_percent_dexinus,
            CASE WHEN prev_dexuseu_converted IS NULL THEN NULL ELSE (DEXUSEU_CONVERTED - prev_dexuseu_converted) / NULLIF(prev_dexuseu_converted, 0) * 100 END AS rate_change_percent_dexuseu_converted,
            CASE WHEN prev_dexusuk_converted IS NULL THEN NULL ELSE (DEXUSUK_CONVERTED - prev_dexusuk_converted) / NULLIF(prev_dexusuk_converted, 0) * 100 END AS rate_change_percent_dexusuk_converted,
            volatility_dexinus,
            volatility_dexuseu_converted,
            volatility_dexusuk_converted
          FROM daily_metrics;
          
          UPDATE ANALYTICS.DAILY_DATA_METRICS
          SET
            rate_change_percent_dexinus = tmp.rate_change_percent_dexinus,
            rate_change_percent_dexuseu_converted = tmp.rate_change_percent_dexuseu_converted,
            rate_change_percent_dexusuk_converted = tmp.rate_change_percent_dexusuk_converted,
            volatility_dexinus = tmp.volatility_dexinus,
            volatility_dexuseu_converted = tmp.volatility_dexuseu_converted,
            volatility_dexusuk_converted = tmp.volatility_dexusuk_converted
          FROM TMP_UPDATED_DAILY_METRICS tmp
          WHERE DAILY_DATA_METRICS.DDATE = tmp.DDATE;
          
          -- Create a temporary table with computed monthly metrics
          CREATE OR REPLACE TEMPORARY TABLE TMP_UPDATED_MONTHLY_METRICS AS
          WITH monthly_metrics AS (
            SELECT
              MDATE,
              EXUSEU_CONVERTED,
              EXUSUK_CONVERTED,
              LAG(EXUSEU_CONVERTED) OVER (ORDER BY MDATE) AS prev_exuseu_converted,
              LAG(EXUSUK_CONVERTED) OVER (ORDER BY MDATE) AS prev_exusuk_converted,
              STDDEV(EXUSEU_CONVERTED) OVER (ORDER BY MDATE) AS volatility_exuseu_converted,
              STDDEV(EXUSUK_CONVERTED) OVER (ORDER BY MDATE) AS volatility_exusuk_converted
            FROM HARMONIZED.HARMONIZED_MONTHLY_TBL
          )
          SELECT
            MDATE,
            CASE WHEN prev_exuseu_converted IS NULL THEN NULL ELSE (EXUSEU_CONVERTED - prev_exuseu_converted) / NULLIF(prev_exuseu_converted, 0) * 100 END AS rate_change_percent_exuseu_converted,
            CASE WHEN prev_exusuk_converted IS NULL THEN NULL ELSE (EXUSUK_CONVERTED - prev_exusuk_converted) / NULLIF(prev_exusuk_converted, 0) * 100 END AS rate_change_percent_exusuk_converted,
            volatility_exuseu_converted,
            volatility_exusuk_converted
          FROM monthly_metrics;
          
          UPDATE ANALYTICS.MONTHLY_DATA_METRICS
          SET
            rate_change_percent_exuseu_converted = tmpm.rate_change_percent_exuseu_converted,
            rate_change_percent_exusuk_converted = tmpm.rate_change_percent_exusuk_converted,
            volatility_exuseu_converted = tmpm.volatility_exuseu_converted,
            volatility_exusuk_converted = tmpm.volatility_exusuk_converted
          FROM TMP_UPDATED_MONTHLY_METRICS tmpm
          WHERE MONTHLY_DATA_METRICS.MDATE = tmpm.MDATE;
          
          RETURN 'Data metrics updated successfully';
        END;
        $$;
    """).collect()

def main():
    try:
        session = Session.builder.configs(snowflake_params).create()
        print("✅ Snowflake connection successful!")
    except Exception as e:
        print("❌ Snowflake connection failed:", e)
        return

    # Create the stored procedure and analytics tables, then call the procedure
    create_stored_procedure(session)
    create_analytics_tables(session)
    session.sql("CALL UPDATE_DATA_METRICS()").collect()
    session.close()
    print("🔄 Snowflake session closed.")

if __name__ == "__main__":
    main()

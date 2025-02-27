import os
from snowflake.snowpark import Session
from dotenv import load_dotenv

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
    # Create a table for daily exchange rate analytics
    session.sql("""
        CREATE OR REPLACE TABLE DAILY_EXCHANGE_RATE_ANALYTICS (
            date STRING,
            currency_pair STRING,
            avg_value FLOAT,
            volatility FLOAT,
            rate_change_percent FLOAT
        )
    """).collect()

    # Create a table for monthly exchange rate analytics
    session.sql("""
        CREATE OR REPLACE TABLE MONTHLY_EXCHANGE_RATE_ANALYTICS (
            month_start_date STRING,
            currency_pair STRING,
            avg_value FLOAT,
            volatility FLOAT,
            rate_change_percent FLOAT
        )
    """).collect()

def create_stored_procedure(session):
    # Define the stored procedure for updating exchange rate analytics
    session.sql("""
        CREATE OR REPLACE PROCEDURE UPDATE_EXCHANGE_RATE_ANALYTICS()
        RETURNS STRING
        LANGUAGE SQL
        AS
        $$
        BEGIN
            -- Calculate daily analytics for each currency pair
            FOR currency_pair IN ('USDINR', 'USDEUR', 'USDGBP') DO
                EXECUTE IMMEDIATE
                'INSERT INTO DAILY_EXCHANGE_RATE_ANALYTICS
                SELECT
                    date,
                    ''' || currency_pair || ''' AS currency_pair,
                    AVG(' || currency_pair || ') AS avg_value,
                    STDDEV(' || currency_pair || ') AS volatility,
                    (MAX(' || currency_pair || ') - MIN(' || currency_pair || ')) / NULLIF(MIN(' || currency_pair || '), 0) * 100 AS rate_change_percent
                FROM HARMONIZED.HARMONIZED_CURRENCY_EXCHANGE
                GROUP BY date';
            END FOR;

            -- Calculate monthly analytics for each currency pair
            FOR currency_pair IN ('USDINR', 'USDEUR', 'USDGBP') DO
                EXECUTE IMMEDIATE
                'INSERT INTO MONTHLY_EXCHANGE_RATE_ANALYTICS
                SELECT
                    DATE_TRUNC(''MONTH'', date) AS month_start_date,
                    ''' || currency_pair || ''' AS currency_pair,
                    AVG(' || currency_pair || ') AS avg_value,
                    STDDEV(' || currency_pair || ') AS volatility,
                    (MAX(' || currency_pair || ') - MIN(' || currency_pair || ')) / NULLIF(MIN(' || currency_pair || '), 0) * 100 AS rate_change_percent
                FROM HARMONIZED.HARMONIZED_CURRENCY_EXCHANGE
                GROUP BY DATE_TRUNC(''MONTH'', date)';
            END FOR;

            RETURN 'Exchange rate analytics updated successfully';
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
    session.sql("CALL UPDATE_EXCHANGE_RATE_ANALYTICS()").collect()

    # Close session
    session.close()
    print("🔄 Snowflake session closed.")

if __name__ == "__main__":
    main()
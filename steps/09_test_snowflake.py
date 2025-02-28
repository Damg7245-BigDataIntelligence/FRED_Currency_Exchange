import pytest
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import os

@pytest.fixture(scope="module")
def snowflake_session():
    # Replace with your actual Snowflake connection parameters
    snowflake_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "database": "FRED_DB",
        "schema": "HARMONIZED",
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
    session = Session.builder.configs(snowflake_params).create()
    yield session
    session.close()

def test_usd_conversion_udf_valid_exchange_rate(snowflake_session):
    exchange_rate = 2
    result = snowflake_session.sql(f"SELECT USD_CONVERSION_UDF({exchange_rate})").collect()[0][0]
    assert result == 0.5, f"Expected 0.5 but got {result}"

def test_usd_conversion_udf_zero_exchange_rate(snowflake_session):
    exchange_rate = 0
    result = snowflake_session.sql(f"SELECT USD_CONVERSION_UDF({exchange_rate})").collect()[0][0]
    assert result is None, f"Expected None but got {result}"

def test_usd_conversion_udf_null_exchange_rate(snowflake_session):
    result = snowflake_session.sql(f"SELECT USD_CONVERSION_UDF(NULL)").collect()[0][0]
    assert result is None, f"Expected None but got {result}"

# def test_forward_fill_udf(snowflake_session):
#     # Assuming FORWARD_FILL_UDF is already registered in Snowflake
#     test_values = ["'1.5'", "'2.3'", "'.'", "'4.7'", "'.'"]
#     test_query = f"SELECT FORWARD_FILL_UDF(ARRAY_CONSTRUCT({', '.join(test_values)}), NULL)"

#     result = snowflake_session.sql(test_query).collect()[0][0]

#     expected_result = [1.5, 2.3, 2.3, 4.7, 4.7]
#     assert result == expected_result, f"Expected {expected_result} but got {result}"



# def test_update_data_metrics(snowflake_session):
#     # Create test data in the tables before running the procedure
#     snowflake_session.sql("""
#         INSERT INTO HARMONIZED.DAILY_TABLE(date, usdtoinr, usdtoeuro, usdto_pound)
#         VALUES
#             ('2025-02-01', 74.5, 0.9, 0.75),
#             ('2025-02-02', 75.0, 0.88, 0.76)
#     """).collect()

#     # Call the stored procedure
#     result = snowflake_session.sql("CALL UPDATE_DATA_METRICS()").collect()[0][0]
    
#     # Assert if the procedure runs successfully
#     assert result == "Data metrics updated successfully"

#     # Check if the data has been updated
#     updated_data = snowflake_session.sql("SELECT * FROM ANALYTICS.DAILY_DATA_METRICS").collect()
#     assert updated_data, "Data was not updated"

# def test_update_data_metrics_edge_case(snowflake_session):
#     # Insert test data with edge cases, such as nulls or missing values
#     snowflake_session.sql("""
#         INSERT INTO HARMONIZED.DAILY_TABLE (date, usdtoinr, usdtoeuro, usdto_pound)
#         VALUES
#             ('2025-02-03', NULL, 0.85, NULL),
#             ('2025-02-04', 76.0, NULL, 0.77)
#     """).collect()

#     # Call the stored procedure
#     result = snowflake_session.sql("CALL UPDATE_DATA_METRICS()").collect()[0][0]
    
#     # Assert if the procedure runs successfully
#     assert result == "Data metrics updated successfully"

#     # Check the updated data
#     updated_data = snowflake_session.sql("SELECT * FROM ANALYTICS.DAILY_DATA_METRICS WHERE date = '2025-02-03'").collect()
#     assert updated_data[0]["rate_change_percent_usdtoinr"] is None, "Rate change not handled for NULL values"



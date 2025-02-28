import streamlit as st
import pandas as pd
import os
from snowflake.snowpark import Session

# Page configuration
st.set_page_config(
    page_title="FRED Currency Exchange Analytics",
    page_icon="💱",
    layout="wide"
)

# App title and description
st.title("💱 FRED Currency Exchange Analytics")
st.markdown("View daily and monthly currency exchange rate analytics from FRED data")

# Function to create Snowflake connection
@st.cache_resource
def create_snowflake_connection():
    # Get connection parameters from environment variables (set in GitHub secrets)
    connection_parameters = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USER"),
        "password": os.environ.get("SNOWFLAKE_PASSWORD"),
        "role": "FRED_ROLE",
        "warehouse": "FRED_WH",
        "database": "FRED_DB"
    }
    
    # Create and return the session
    return Session.builder.configs(connection_parameters).create()

# Connect to Snowflake
try:
    session = create_snowflake_connection()
    st.success("Connected to Snowflake successfully!")
except Exception as e:
    st.error(f"Failed to connect to Snowflake: {e}")
    st.stop()

# Environment selection
env = st.selectbox("Select Environment", ["DEV", "PROD"], index=0)
analytics_schema = f"{env}_ANALYTICS_SCHEMA"

# Data type selection
data_type = st.selectbox("Select Data Type", ["Daily", "Monthly"], index=0)

# Function to fetch data based on selection
def fetch_data(data_type, schema):
    if data_type == "Daily":
        query = f"""
        SELECT * FROM {schema}.DAILY_DATA_METRICS 
        ORDER BY DDATE DESC 
        LIMIT 20
        """
        df = session.sql(query).to_pandas()
        date_col = "DDATE"
        metrics = {
            "US-India Exchange Rate": "DEXINUS",
            "US-EU Exchange Rate": "DEXUSEU_CONVERTED",
            "US-UK Exchange Rate": "DEXUSUK_CONVERTED"
        }
    else:  # Monthly
        query = f"""
        SELECT * FROM {schema}.MONTHLY_DATA_METRICS 
        ORDER BY MDATE DESC 
        LIMIT 20
        """
        df = session.sql(query).to_pandas()
        date_col = "MDATE"
        metrics = {
            "US-India Exchange Rate": "EXINUS",
            "US-EU Exchange Rate": "EXUSEU_CONVERTED",
            "US-UK Exchange Rate": "EXUSUK_CONVERTED"
        }
    
    # Reverse the order for plotting (oldest to newest)
    df_plot = df.sort_values(by=date_col)
    
    return df, df_plot, date_col, metrics

# Fetch data
try:
    df, df_plot, date_col, metrics = fetch_data(data_type, analytics_schema)
    
    # Display the data table
    st.subheader(f"{data_type} Currency Exchange Data")
    st.dataframe(df, use_container_width=True)
    
    # Create visualizations using Streamlit's native charts
    st.subheader(f"{data_type} Currency Exchange Trends")
    
    # Create tabs for different visualizations
    tab1, tab2, tab3 = st.tabs(["Exchange Rates", "Rate Changes", "Volatility"])
    
    with tab1:
        # Line chart for exchange rates
        st.line_chart(
            df_plot, 
            x=date_col,
            y=list(metrics.values()),
            use_container_width=True
        )
    
    with tab2:
        # Rate change percentages
        if data_type == "Daily":
            change_metrics = {
                "US-India Rate Change %": "rate_change_percent_dexinus",
                "US-EU Rate Change %": "rate_change_percent_dexuseu_converted",
                "US-UK Rate Change %": "rate_change_percent_dexusuk_converted"
            }
        else:
            change_metrics = {
                "US-India Rate Change %": "rate_change_percent_exinus",
                "US-EU Rate Change %": "rate_change_percent_exuseu_converted",
                "US-UK Rate Change %": "rate_change_percent_exusuk_converted"
            }
        
        st.bar_chart(
            df_plot,
            x=date_col,
            y=list(change_metrics.values()),
            use_container_width=True
        )
    
    with tab3:
        # Volatility metrics
        if data_type == "Daily":
            volatility_metrics = {
                "US-India Volatility": "volatility_dexinus",
                "US-EU Volatility": "volatility_dexuseu_converted",
                "US-UK Volatility": "volatility_dexusuk_converted"
            }
        else:
            volatility_metrics = {
                "US-India Volatility": "volatility_exinus",
                "US-EU Volatility": "volatility_exuseu_converted",
                "US-UK Volatility": "volatility_exusuk_converted"
            }
        
        st.area_chart(
            df_plot,
            x=date_col,
            y=list(volatility_metrics.values()),
            use_container_width=True
        )

except Exception as e:
    st.error(f"Error fetching data: {e}")

# Footer
st.markdown("---")
st.markdown("Data source: Federal Reserve Economic Data (FRED)")
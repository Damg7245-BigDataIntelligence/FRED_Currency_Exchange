from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables
load_dotenv()

# Snowflake connection parameters
snowflake_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": "RAW_DAILY",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

# Establish Snowflake session
try:
    session = Session.builder.configs(snowflake_params).create()
    print("✅ Snowflake connection successful!")
except Exception as e:
    print("❌ Snowflake connection failed:", e)
    exit()

# Fetch data from Snowflake into Pandas DataFrame and close session
df = pd.DataFrame.from_records(session.table("RAW_DEXINUS").collect(), columns=[col.name for col in session.table("RAW_DEXINUS").schema.fields])
session.close()

# Clean column names (strip extra quotes)
df.columns = [col.strip('"') for col in df.columns]
print("\n📌 Cleaned Column Names:", df.columns)

# Convert 'date' column to datetime and sort by date
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(by="date")

# Generate full date range from min to max date
full_date_range = pd.date_range(start=df["date"].min(), end=df["date"].max(), freq='D')

# Create a new DataFrame with missing dates filled
existing_dates = set(df["date"])
new_rows = []
last_value = None
for single_date in full_date_range:
    if single_date in existing_dates:
        last_value = df.loc[df["date"] == single_date, "value"].values[0]
    else:
        new_rows.append({"date": single_date, "value": last_value})

# Convert missing dates into a DataFrame and combine with original data
missing_df = pd.DataFrame(new_rows)
final_df = pd.concat([df, missing_df]).sort_values("date").reset_index(drop=True)

# Replace 0.00 values with the previous day's value
for i in range(1, len(final_df)):
    if final_df.loc[i, "value"] == 0.00:
        final_df.loc[i, "value"] = final_df.loc[i - 1, "value"]

print("\n📌 Data After Filling Missing Dates and Replacing 0.00 Values:")
print(final_df.head(30))


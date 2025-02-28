--!jinja

-- Create schemas based on environment
CREATE SCHEMA IF NOT EXISTS "FRED_DB"."{{env}}_RAW_SCHEMA";
CREATE SCHEMA IF NOT EXISTS "FRED_DB"."{{env}}_HARMONIZED_SCHEMA";
CREATE SCHEMA IF NOT EXISTS "FRED_DB"."{{env}}_ANALYTICS_SCHEMA";

-- Deploy load_raw_data notebook
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_SCHEMA"."{{env}}_load_raw_data"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/load_raw_data/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'load_raw_data.ipynb';

ALTER NOTEBOOK "FRED_DB"."{{env}}_SCHEMA"."{{env}}_load_raw_data" ADD LIVE VERSION FROM LAST;

-- Deploy harmonize_data notebook
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_SCHEMA"."{{env}}_harmonize_data"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/harmonized_data/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'harmonize_data.ipynb';

ALTER NOTEBOOK "FRED_DB"."{{env}}_SCHEMA"."{{env}}_harmonize_data" ADD LIVE VERSION FROM LAST;

-- Deploy analytics notebook
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_SCHEMA"."{{env}}_analytics"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/analytics/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'analytics.ipynb';

ALTER NOTEBOOK "FRED_DB"."{{env}}_SCHEMA"."{{env}}_analytics" ADD LIVE VERSION FROM LAST;

-- Deploy UDFs and stored procedures notebook

-- Deploy orchestration DAG notebook
-- CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_SCHEMA"."{{env}}_orchestration"')
--     FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/notebooks/orchestration/'
--     QUERY_WAREHOUSE = 'FRED_WH'
--     MAIN_FILE = 'orchestration.ipynb';

-- ALTER NOTEBOOK "FRED_DB"."{{env}}_SCHEMA"."{{env}}_orchestration" ADD LIVE VERSION FROM LAST;
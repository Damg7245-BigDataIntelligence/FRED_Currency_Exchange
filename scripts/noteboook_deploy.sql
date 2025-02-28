--!jinja

-- Create schemas based on environment
CREATE SCHEMA IF NOT EXISTS "FRED_DB"."{{env}}_RAW_SCHEMA";
CREATE SCHEMA IF NOT EXISTS "FRED_DB"."{{env}}_HARMONIZED_SCHEMA";
CREATE SCHEMA IF NOT EXISTS "FRED_DB"."{{env}}_ANALYTICS_SCHEMA";

-- Deploy load_raw_data notebook to INTEGRATIONS schema
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."INTEGRATIONS"."{{env}}_load_raw_data"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/load_raw_data/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'load_raw_data.ipynb';

ALTER NOTEBOOK "FRED_DB"."INTEGRATIONS"."{{env}}_load_raw_data" ADD LIVE VERSION FROM LAST;

-- Deploy harmonize_data notebook to INTEGRATIONS schema
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."INTEGRATIONS"."{{env}}_harmonize_data"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/harmonized_data/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'harmonize_data.ipynb';

ALTER NOTEBOOK "FRED_DB"."INTEGRATIONS"."{{env}}_harmonize_data" ADD LIVE VERSION FROM LAST;

-- Deploy analytics notebook to INTEGRATIONS schema
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."INTEGRATIONS"."{{env}}_analytics"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/analytics/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'analytics.ipynb';

ALTER NOTEBOOK "FRED_DB"."INTEGRATIONS"."{{env}}_analytics" ADD LIVE VERSION FROM LAST;

-- Deploy orchestration DAG notebook to INTEGRATIONS schema
-- CREATE OR REPLACE PROCEDURE INTEGRATIONS.CREATE_CURRENCY_DAG()
-- RETURNS VARCHAR
-- LANGUAGE SQL
-- AS
-- $$
-- DECLARE
--   ENV VARCHAR DEFAULT '{{env}}';
--   warehouse_name VARCHAR DEFAULT 'FRED_WH';
--   dag_name VARCHAR DEFAULT CONCAT(ENV, '_FRED_CURRENCY_PIPELINE');
-- BEGIN
--   -- Create the DAG using Snowflake's SQL API
--   EXECUTE IMMEDIATE 'CREATE OR REPLACE TASK ' || dag_name || '_LOAD_RAW_DATA_TASK
--     WAREHOUSE = ''' || warehouse_name || '''
--     SCHEDULE = ''USING CRON 0 0 * * * UTC''
--     AS
--     EXECUTE NOTEBOOK "FRED_DB"."INTEGRATIONS"."' || ENV || '_load_raw_data"()';
    
--   EXECUTE IMMEDIATE 'CREATE OR REPLACE TASK ' || dag_name || '_HARMONIZE_DATA_TASK
--     WAREHOUSE = ''' || warehouse_name || '''
--     AFTER ' || dag_name || '_LOAD_RAW_DATA_TASK
--     AS
--     EXECUTE NOTEBOOK "FRED_DB"."INTEGRATIONS"."' || ENV || '_harmonize_data"()';
    
--   EXECUTE IMMEDIATE 'CREATE OR REPLACE TASK ' || dag_name || '_ANALYTICS_TASK
--     WAREHOUSE = ''' || warehouse_name || '''
--     AFTER ' || dag_name || '_HARMONIZE_DATA_TASK
--     AS
--     EXECUTE NOTEBOOK "FRED_DB"."INTEGRATIONS"."' || ENV || '_analytics"()';
    
--   -- Enable the tasks
--   EXECUTE IMMEDIATE 'ALTER TASK ' || dag_name || '_LOAD_RAW_DATA_TASK RESUME';
--   EXECUTE IMMEDIATE 'ALTER TASK ' || dag_name || '_HARMONIZE_DATA_TASK RESUME';
--   EXECUTE IMMEDIATE 'ALTER TASK ' || dag_name || '_ANALYTICS_TASK RESUME';
  
--   RETURN 'Created and enabled DAG: ' || dag_name;
-- END;
-- $$;

-- -- Call the procedure to create the DAG
-- CALL INTEGRATIONS.CREATE_CURRENCY_DAG();
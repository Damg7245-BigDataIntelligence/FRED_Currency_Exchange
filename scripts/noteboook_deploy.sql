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
EXECUTE IMMEDIATE $$
DECLARE
  ENV VARCHAR DEFAULT '{{env}}';
  warehouse_name VARCHAR DEFAULT 'FRED_WH';
  dag_name VARCHAR DEFAULT CONCAT(ENV, '_FRED_CURRENCY_PIPELINE');
BEGIN
  -- Create the DAG using Snowflake's DAG API
  EXECUTE IMMEDIATE 'CREATE OR REPLACE PROCEDURE INTEGRATIONS.CREATE_CURRENCY_DAG()
  RETURNS VARCHAR
  LANGUAGE PYTHON
  RUNTIME_VERSION=3.8
  PACKAGES=(''snowflake-snowpark-python'')
  HANDLER=''create_dag''
  AS
  $$
  def create_dag(session):
      from snowflake.core import Root
      from snowflake.core.task.dagv1 import DAGOperation, DAG, DAGTask
      from datetime import timedelta
      
      # Environment variables
      ENV = "' || ENV || '"
      warehouse_name = "' || warehouse_name || '"
      dag_name = "' || dag_name || '"
      
      api_root = Root(session)
      schema = api_root.databases["FRED_DB"].schemas["INTEGRATIONS"]
      dag_op = DAGOperation(schema)
      
      # Define the DAG
      with DAG(dag_name, schedule=timedelta(days=1), warehouse=warehouse_name) as dag:
          # Define tasks for each step in the pipeline
          
          # 1. Load Raw Data Task
          load_raw_data_task = DAGTask(
              "LOAD_RAW_DATA_TASK", 
              definition=f"""EXECUTE NOTEBOOK \\"FRED_DB\\".\\\"INTEGRATIONS\\".\\\"' || ENV || '_load_raw_data\\"()""", 
              warehouse=warehouse_name
          )
          
          # 2. Harmonize Data Task
          harmonize_data_task = DAGTask(
              "HARMONIZE_DATA_TASK", 
              definition=f"""EXECUTE NOTEBOOK \\"FRED_DB\\".\\\"INTEGRATIONS\\".\\\"' || ENV || '_harmonize_data\\"()""", 
              warehouse=warehouse_name
          )
          
          # 3. Analytics Task
          analytics_task = DAGTask(
              "ANALYTICS_TASK", 
              definition=f"""EXECUTE NOTEBOOK \\"FRED_DB\\".\\\"INTEGRATIONS\\".\\\"' || ENV || '_analytics\\"()""", 
              warehouse=warehouse_name
          )
      
          # Define the dependencies between the tasks
          load_raw_data_task >> harmonize_data_task >> analytics_task
      
      # Create the DAG in Snowflake
      dag_op.deploy(dag, mode="orreplace")
      
      # Return success message
      return f"Created DAG: {dag_name}"
  $$';
  
  -- Call the procedure to create the DAG
  CALL INTEGRATIONS.CREATE_CURRENCY_DAG();
END;
$$;

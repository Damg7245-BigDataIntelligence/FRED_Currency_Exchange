--!jinja
 
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_{{schema1}}"."{{env}}_load_raw_data"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/load_raw_data/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'load_raw_data.ipynb';
 
ALTER NOTEBOOK "FRED_DB"."{{env}}_{{schema1}}"."{{env}}_load_raw_data" ADD LIVE VERSION FROM LAST;
 
-- Deploy harmonize_data notebook to INTEGRATIONS schema
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_{{schema2}}"."{{env}}_harmonize_data"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/harmonized_data/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'harmonize_data.ipynb';
 
ALTER NOTEBOOK "FRED_DB"."{{env}}_{{schema2}}"."{{env}}_harmonize_data" ADD LIVE VERSION FROM LAST;
 
-- Deploy analytics notebook to INTEGRATIONS schema
CREATE OR REPLACE NOTEBOOK IDENTIFIER('"FRED_DB"."{{env}}_{{schema3}}"."{{env}}_analytics"')
    FROM '@"FRED_DB"."INTEGRATIONS"."FRED_GIT_REPO"/branches/"{{branch}}"/steps/analytics/'
    QUERY_WAREHOUSE = 'FRED_WH'
    MAIN_FILE = 'analytics.ipynb';
 
ALTER NOTEBOOK "FRED_DB"."{{env}}_{{schema3}}"."{{env}}_analytics" ADD LIVE VERSION FROM LAST;

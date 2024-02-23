
CREATE OR REPLACE FUNCTION INTERNAL.PARSE_CLOUD()
RETURNS TEXT
LANGUAGE SQL
AS
$$
    -- Remove the REGION_GROUP when present from current_region()
    select UPPER(cloud) from internal.regions where upper(SF_REGION) = upper(SPLIT_PART(CURRENT_REGION(), '.', -1))
$$;

CREATE OR REPLACE FUNCTION INTERNAL.PARSE_REGION()
RETURNS TEXT
LANGUAGE SQL
AS
$$
    -- Remove the REGION_GROUP when present from current_region()
    select UPPER(region) from internal.regions where upper(SF_REGION) = upper(SPLIT_PART(CURRENT_REGION(), '.', -1))
$$;

-- Access to the SUNDECK database is given to application_package in deploy.py
-- SHARING.GLOBAL_QUERY_HISTORY is already filtered on SNOWFLAKE_ACCOUNT_LOCATOR = CURRENT_ACCOUNT()
CREATE OR REPLACE VIEW REPORTING.SUNDECK_QUERY_HISTORY
   AS SELECT * from SHARING.GLOBAL_QUERY_HISTORY WHERE
     UPPER(SUNDECK_ACCOUNT_ID) = (select UPPER(value) from internal.config where key = 'tenant_id') and
     UPPER(SNOWFLAKE_REGION) = (select internal.parse_region()) and
     UPPER(SNOWFLAKE_CLOUD) = (select internal.parse_cloud());

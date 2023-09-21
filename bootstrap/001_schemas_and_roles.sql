
CREATE SCHEMA IF NOT EXISTS INTERNAL_REPORTING;
CREATE SCHEMA IF NOT EXISTS INTERNAL_REPORTING_MV;
-- Python UDFs and procedures may only import code when in a versioned schema
CREATE OR ALTER VERSIONED SCHEMA INTERNAL_PYTHON;

CREATE APPLICATION ROLE IF NOT EXISTS ADMIN;
CREATE APPLICATION ROLE IF NOT EXISTS READ_ONLY;
CREATE APPLICATION ROLE IF NOT EXISTS PUBLIC_;

CREATE SCHEMA IF NOT EXISTS ACCOUNT_USAGE;
CREATE SCHEMA IF NOT EXISTS ORGANIZATION_USAGE;

CREATE SCHEMA IF NOT EXISTS TASKS;
GRANT USAGE ON SCHEMA TASKS TO APPLICATION ROLE ADMIN;

CREATE OR ALTER VERSIONED SCHEMA CATALOG;
GRANT USAGE ON SCHEMA CATALOG TO APPLICATION ROLE ADMIN;
GRANT USAGE ON SCHEMA CATALOG TO APPLICATION ROLE READ_ONLY;

CREATE OR ALTER VERSIONED SCHEMA TOOLS;
GRANT USAGE ON SCHEMA TOOLS TO APPLICATION ROLE ADMIN;
GRANT USAGE ON SCHEMA TOOLS TO APPLICATION ROLE READ_ONLY;
GRANT USAGE ON SCHEMA TOOLS TO APPLICATION ROLE PUBLIC_;

CREATE OR ALTER VERSIONED SCHEMA REPORTING;
GRANT USAGE ON SCHEMA REPORTING TO APPLICATION ROLE ADMIN;
GRANT USAGE ON SCHEMA REPORTING TO APPLICATION ROLE READ_ONLY;

CREATE OR ALTER VERSIONED SCHEMA ADMIN;
GRANT USAGE ON SCHEMA ADMIN TO APPLICATION ROLE ADMIN;

create view catalog.config if not exists as select * from internal.config;

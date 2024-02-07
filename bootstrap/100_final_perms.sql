
grant select on all views in schema REPORTING to APPLICATION ROLE ADMIN;
grant select on all views in schema REPORTING to APPLICATION ROLE READ_ONLY;
grant usage on all functions in schema REPORTING to APPLICATION ROLE ADMIN;
grant usage on all functions in schema REPORTING to APPLICATION ROLE READ_ONLY;

grant usage on all functions in schema ADMIN to APPLICATION ROLE ADMIN;
grant usage on all procedures in schema ADMIN to APPLICATION ROLE ADMIN;

grant MONITOR, OPERATE on all tasks in schema TASKS to APPLICATION ROLE ADMIN;

grant select on all views in schema CATALOG to APPLICATION ROLE ADMIN;
grant select on all views in schema CATALOG to APPLICATION ROLE READ_ONLY;

grant usage on all functions in schema TOOLS to APPLICATION ROLE READ_ONLY;
grant usage on all functions in schema TOOLS to APPLICATION ROLE ADMIN;
grant usage on all functions in schema TOOLS to APPLICATION ROLE PUBLIC_;

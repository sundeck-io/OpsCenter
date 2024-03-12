

create or replace function admin.list_brokers()
    returns TABLE (name VARCHAR, is_default VARCHAR, default_warehouse VARCHAR, hostname VARCHAR, last_modified_at VARCHAR, ddl VARCHAR, results_path VARCHAR, comment VARCHAR)
as
$$
    SELECT
    value:name::string,
    value:isDefault::string,
    value:defaultWarehouse::string,
    value:flowUrl::string,
    value:lastModifiedDate::string,
    value:ddl::string,
    value:resultPath::string,
    value:comment::string
    FROM TABLE(FLATTEN(input => internal.wrapper_list_brokers(object_construct())))
$$;

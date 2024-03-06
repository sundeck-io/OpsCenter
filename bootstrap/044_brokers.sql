

create or replace function admin.list_brokers()
    returns TABLE (flow_name VARCHAR, flow_is_default VARCHAR, flow_default_warehouse VARCHAR, flow_hostname VARCHAR, flow_last_modified_at VARCHAR, flow_ddl VARCHAR, flow_results_path VARCHAR, flow_comment VARCHAR)
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



create or replace function admin.list_routines()
    returns TABLE (routine_name VARCHAR, routine_namespace VARCHAR, routine_created_at VARCHAR, routine_last_modified_at VARCHAR, routine_ref_count INT, routine_ddl VARCHAR, routine_comment VARCHAR)
as
$$
    SELECT
    value:name::string,
    value:namespace::string,
    value:createdAt::string,
    value:lastModifiedAt::string,
    value:refCount::int,
    value:ddl::string,
    value:comment::string
    FROM TABLE(FLATTEN(input => internal.wrapper_list_routines(object_construct())))
$$;

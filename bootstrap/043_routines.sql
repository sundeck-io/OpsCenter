

create or replace function admin.list_routines()
    returns TABLE (name VARCHAR, namespace VARCHAR, refCount INT, schema VARCHAR, comment VARCHAR, childHooks VARCHAR)
as
$$
    SELECT
    value:name::string,
    value:namespace::string,
    value:refCount::int,
    value:schema::string,
    value:comment::string,
    value:childHooks::string
    FROM TABLE(FLATTEN(input => internal.wrapper_list_routines(object_construct())))
$$;

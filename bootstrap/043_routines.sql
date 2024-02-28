
create or replace function admin.list_routines()
    returns object
as
$$
    internal.wrapper_list_routines(object_construct())
$$;

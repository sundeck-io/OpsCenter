
create or replace function internal.wrap_simple_validation(condition text)
returns text
as
$$
    'select case when ' || condition || ' then 1 else 0 end as result from (select parse_json(?) as f)'
$$;

create or replace procedure admin.validate(obj object, validation_table text, is_create boolean)
returns text
language sql
execute as owner
as
$$
begin
-- Select the validation rows. In a create, choose all. In an update, omit rows which are for create_only.
let validation_query text := (select tools.templatejs(
    'select * from internal.validate_{table} where iff({is_create}, true, NOT COALESCE(obj[\'create_only\'], FALSE))',
    {'table': :validation_table, 'is_create': :is_create}));
let rs resultset := (execute immediate :validation_query);
let c cursor for rs;
for rec in c do
    begin
        let q varchar;
        if (rec['complex']) then
            -- "complex" validations provide a single row with a "result" column but do so with their own SQL statement.
            q := rec['sql'];
        else
            -- "simple" validations have a condition which is injected in this sql statement
            q := (select internal.wrap_simple_validation(:rec['sql']));
        end if;

        let res resultset := (execute immediate :q using (obj));
        let cc cursor for res;
        for rr in cc do
            if (not rr.result) then
                return 'Error in validation: ' || rec['message'];
            end if;
        end for;
    exception
        when other then
            return 'Error in validation: ' || rec['message'] || ': ' || sqlerrm;
    end;
end for;
end;
$$;

create or replace procedure admin.write(obj object, tbl text, key text)
returns text
language sql
execute as owner
as
begin
    let keys array := object_keys(:obj);

    -- The USING portion of the merge statement.
    let using_select_cols array := (select array_agg(tools.templatejs('obj[\'{col}\'] as {col}', {'col': value})) from table(flatten(input=>:keys)));
    let using_expr text := (select array_to_string(:using_select_cols, ', '));

    -- The UPDATE portion of the merge statement.
    let update_cols array := (select array_agg(tools.templatejs('dest.{col} = src.{col}', {'col': value})) from table(flatten(input=>:keys)));
    let update_expr text := (select array_to_string(:update_cols, ', '));

    -- The INSERT portion of the merge statement.
    let insert_target_expr text := (select array_to_string(:keys, ', '));
    let insert_query_cols array := (select array_agg(tools.templatejs('src.{col}', {'col': value})) from table(flatten(input=>:keys)));
    let insert_query_expr text := (select array_to_string(:insert_query_cols, ', '));

    let merge_tmpl varchar := $$MERGE INTO internal.{table} dest USING (
SELECT {using_expr} FROM (SELECT PARSE_JSON(?) AS obj)) src
ON dest.{key} = src.{key}
WHEN MATCHED THEN
    UPDATE SET {update_expr}
WHEN NOT MATCHED THEN
    INSERT ({insert_target_expr}) values ({insert_query_expr})
$$;
    let merge_stmt varchar := (select tools.templatejs(:merge_tmpl,
        {'table': :tbl, 'using_expr': :using_expr, 'key': :key, 'update_expr': :update_expr,
            'insert_target_expr': :insert_target_expr, 'insert_query_expr': :insert_query_expr}));
    execute immediate :merge_stmt using (obj);
    return merge_stmt;
end;

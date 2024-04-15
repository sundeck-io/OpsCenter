
create or replace procedure admin.validate(obj object, validation_table text, is_create boolean)
returns text
language sql
execute as owner
as
$$
begin
    -- Select the validation rows. In a create, choose all. In an update, omit rows which are for create_only.
    let qualified_table text := (select 'internal.validate_' || :validation_table);
    let validation_query text := (select 'select * from identifier(?) where iff(?::BOOLEAN, true, NOT COALESCE(obj[\'create_only\'], FALSE))');
    let rs resultset := (execute immediate :validation_query using (qualified_table, is_create));
    let c cursor for rs;
    for rec in c do
        begin
            let q varchar;
            if (rec['complex']) then
                -- "complex" validations provide a single row with a "result" column but do so with their own SQL statement.
                q := rec['sql'];
            else
                -- "simple" validations have a condition which is injected in this sql statement
                q := (select validation.wrap_simple_condition(:rec['sql']));
            end if;

            let validation_result resultset := (execute immediate :q using (obj));
            let cc cursor for validation_result;
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

    -- The USING portion of the merge statement, e.g. 'obj['col1'] as col1, ...'
    let using_select_cols array := (select array_agg('obj[\'' || value || '\'] as ' || value) from table(flatten(input=>:keys)));
    let using_expr text := (select array_to_string(:using_select_cols, ', '));

    -- The UPDATE portion of the merge statement, e.g. 'dest.col1 = src.col1, ...'
    let update_cols array := (select array_agg('dest.' || value || ' = src.' || value) from table(flatten(input=>:keys)));
    let update_expr text := (select array_to_string(:update_cols, ', '));

    -- The INSERT portion of the merge statement
    let insert_target_expr text := (select array_to_string(:keys, ', '));
    -- e.g. src.col1, src.col2, ...
    let insert_query_cols array := (select array_agg('src.' || value) from table(flatten(input=>:keys)));
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
end;

CREATE OR REPLACE FUNCTION VALIDATION.IS_NULL(name text)
    RETURNS text
    COMMENT='Validates that a field in an object is null'
AS
$$
    'TO_CHAR(f:' || name || ') is null'
$$;

CREATE OR REPLACE FUNCTION VALIDATION.NOT_NULL(name text)
    RETURNS text
    COMMENT='Validates that a field in an object is not null'
AS
$$
    'TO_CHAR(f:' || name || ') is not null'
$$;

CREATE OR REPLACE FUNCTION VALIDATION.LABEL_EXISTS()
    RETURNS text
AS
$$
    '(select count(*) > 0 from INTERNAL.LABELS where group_name is null and name = f:name)'
$$;

CREATE OR REPLACE FUNCTION VALIDATION.LABEL_ABSENT()
    RETURNS text
AS
$$
    'NOT(' || VALIDATION.LABEL_EXISTS() || ')'
$$;

CREATE OR REPLACE FUNCTION VALIDATION.GROUPED_LABEL_ABSENT()
    RETURNS TEXT
AS
$$
    'select count(*) = 0 from INTERNAL.LABELS where group_name = f:group_name and name = f:name'
$$;

CREATE OR REPLACE FUNCTION VALIDATION.GROUP_NAME_UNIQUE()
    RETURNS TEXT
AS
$$
    'select count(*) = 0 from internal.labels where name = f:group_name and group_name is null'
$$;

CREATE OR REPLACE PROCEDURE VALIDATION.VALID_LABEL_CONDITION(input varchar)
    RETURNS BOOLEAN
    LANGUAGE SQL
AS
BEGIN
    let c varchar := (select parse_json(:input):condition);
    let stmt varchar := (select 'select ' || :c || ' from reporting.enriched_query_history limit 1');
    execute immediate :stmt;
    -- if the condition is invalid, we let the exception propagate.
    return true;
END;

CREATE OR REPLACE PROCEDURE VALIDATION.IS_VALID_LABEL_NAME(input varchar)
    RETURNS BOOLEAN
    LANGUAGE SQL
    comment='Validates that the label name does not exist as a column in the reporting.enriched_query_history table'
AS
BEGIN
    let n varchar := (select parse_json(:input):name);
    let stmt varchar := (select 'select ' || :n || ' from reporting.enriched_query_history where false');
    execute immediate stmt;
    return false;
EXCEPTION
    WHEN STATEMENT_ERROR THEN
        return true;
    WHEN OTHER then
        return false;
END;

create or replace function VALIDATION.wrap_simple_condition(condition text)
returns text
as
$$
    'select case when ' || condition || ' then 1 else 0 end as result from (select parse_json(?) as f)'
$$;

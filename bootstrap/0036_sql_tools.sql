
create or replace procedure admin.validate(obj object, validation_table text, is_create boolean)
returns text
language sql
execute as owner
as
$$
begin
let rs resultset := (execute immediate 'select * from internal.validate_' || :validation_table);
let c cursor for rs;
for rec in c do
    begin
        if (rec.create_only and not is_create) then
            -- if the validation is only for create operations, skip it. e.g. if we update label and don't change the
            -- name, we want to skip the uniqueness validations because they would incorrectly fail.
            continue;
        end if;

        -- "simple" validations have a condition which is injected in this sql statement
        let q varchar := 'select case when ' || rec.sql || ' then 1 else 0 end as result from (select parse_json(?) as f)';
        if (not rec.simple) then
            -- "complex" validations provide a single row with a "result" column but do so with their own SQL statement.
            q := rec.sql;
        end if;

        let res resultset := (execute immediate :q using (obj));
        let cc cursor for res;
        for rr in cc do
            if (not rr.result) then
                return 'Error in validation: ' || rec.message;
            end if;
        end for;
    exception
        when other then
            return 'Error in validation: ' || rec.message || ': ' || sqlerrm;
    end;
end for;
end;
$$;

create or replace procedure admin.write(obj object, tbl text, key text)
returns text
language sql
execute as owner
as
$$
begin
    let keys array := object_keys(:obj);
    let cols varchar := keys[0];
    let vals varchar := 'obj[\'' || keys[0] || '\'] as ' || keys[0];
    let ins_cols varchar := 'src.' || keys[0];
    let set_cmds varchar := 'set dest.' || keys[0] || ' = src.' || keys[0];
    for i in 1 to (array_size(:keys) - 1) do
        cols := cols || ',' || keys[i];
        vals := vals || ',' || 'obj[\'' || keys[i] || '\'] as ' || keys[i];
        ins_cols := ins_cols || ', src.' || keys[i];
        set_cmds := set_cmds || ', dest.' || keys[i] || ' = src.' || keys[i];
    end for;
    let stmt varchar := 'merge into internal.' || :tbl || ' dest using (select ' || :vals || ' from (select parse_json(?) as obj)) src on dest.' || :key || ' = src.' || :key|| ' when matched then update ' || :set_cmds || ' when not matched then insert (' || :cols || ') values (' || :ins_cols || ')' ;
    execute immediate :stmt using (obj);
    return :stmt;
end;
$$;

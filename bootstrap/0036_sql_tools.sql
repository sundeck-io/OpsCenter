
create or replace procedure admin.validate(obj object, validation_table text)
returns text
language sql
execute as owner
as
$$
begin
let rs resultset := (execute immediate 'select * from internal.validate_' || :validation_table);
let c cursor for rs;
for rec in c do
    let q varchar := 'select case when ' || rec.sql || ' then 1 else 0 end as result from (select parse_json(?) as f)';
    if (not rec.simple) then
        q := rec.sql;
    end if;
    begin
        let res resultset := (execute immediate :q using (obj));
        let cc cursor for res;
        for rr in cc do
            if (not rr.result) then
                return 'Error in validation: ' || rec.message;
            end if;
        end for;
    exception
        when others then
            return 'Error in validation: ' || rec.message || ' exception ' || sqlerrm;
    end;
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

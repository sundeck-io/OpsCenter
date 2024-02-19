
create table if not exists internal.reference_management (
    ref_name string,
    operation string,
    ref_or_alias string
);

create or replace view catalog.references as select * from internal.reference_management;

create or replace procedure admin.update_reference(ref_name string, operation string, ref_or_alias string)
 returns string
 as $$
begin
  SYSTEM$LOG_INFO('Updating reference: ' || ref_name || ' operation: ' || operation || ' ref_or_alias: ' || ref_or_alias);
  case (operation)
    when 'ADD' then
       select system$set_reference(:ref_name, :ref_or_alias);
       insert into internal.reference_management (ref_name, operation, ref_or_alias) values (:ref_name, 'Running external functions setup proc.', :ref_or_alias);
    when 'REMOVE' then
       select system$remove_reference(:ref_name, :ref_or_alias);
       delete from internal.reference_management where name = :ref_name;
    when 'CLEAR' then
       select system$remove_all_references(:ref_name);
       delete from internal.reference_management where name = :ref_name;
    else
       return 'Unknown operation: ' || operation;
  end case;
  return 'Success';
end;
$$;

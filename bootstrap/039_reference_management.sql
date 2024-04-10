
drop view if exists catalog.references;
drop table if exists internal.reference_management;

create or replace procedure admin.update_reference(ref_name string, operation string, ref_or_alias string)
 returns string
 language sql
 as
begin
  SYSTEM$LOG_INFO('Updating reference: ' || ref_name || ' operation: ' || operation || ' ref_or_alias: ' || ref_or_alias);
  case (operation)
    when 'ADD' then
       select system$set_reference(:ref_name, :ref_or_alias);
    when 'REMOVE' then
       select system$remove_reference(:ref_name, :ref_or_alias);
    when 'CLEAR' then
       select system$remove_all_references(:ref_name);
    else
       return 'Unknown operation: ' || operation;
  end case;
  return '';
end;


CREATE OR REPLACE PROCEDURE internal.migrate_if_necessary(view_schema STRING, view_name STRING, table_schema STRING, table_name STRING)
    RETURNS STRING
    LANGUAGE SQL
    COMMENT = 'Looks at a defined view and the associated materialized table. If the schema of the view has added columns since the last time the table has been materialized, append those columns to the table. This expects schemas to be safely updated (no column changes/removals, only columns added).'
    AS
DECLARE
  VIEW_NOT_EXISTS EXCEPTION (-20005, 'The referenced view does not exist');
  TABLE_NOT_EXISTS EXCEPTION (-20004, 'The referenced table does not exist');
  OUT_OF_SYNC EXCEPTION (-20003, 'The table has columns that conflict with those defined in the view.');
BEGIN
  SYSTEM$LOG_INFO('Running migration for ' || :view_schema || '.' || :view_name || ' and ' || :table_schema || '.' || :table_name);
  SYSTEM$SET_SPAN_ATTRIBUTES({'view_schema': (:view_schema), 'view_name': (:view_name), 'table_schema': (:table_schema), 'table_name': (:table_name)});
  let view_exists boolean := (SELECT count(*) = 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = :view_schema AND TABLE_NAME = :view_name);
  IF (NOT view_exists) THEN
      SYSTEM$LOG_ERROR('View does not exist' || :view_schema || '.' || :view_name);
      SYSTEM$ADD_EVENT('view does not exist');
      RAISE VIEW_NOT_EXISTS;
  END IF;

  let table_exists boolean := (SELECT count(*) = 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :table_schema AND TABLE_NAME = :table_name);
  IF (NOT table_exists) THEN
      SYSTEM$LOG_ERROR('Table does not exist' || :table_schema || '.' || :table_name);
      SYSTEM$ADD_EVENT('table does not exist');
      RAISE TABLE_NOT_EXISTS;
  END IF;

  let alter_statement string := 'ALTER TABLE "' || :table_schema || '"."' || :table_name || '"';

  let alter_table_drop_column string := '';

  -- Any columns which exist in the table but not in the view should be dropped, regardless of the ordinal_position.
  let columns_to_drop string := (
      SELECT LISTAGG('"' || COLUMN_NAME || '" ' , ', ')
      FROM (
      SELECT COLUMN_NAME, DATA_TYPE, ROW_NUMBER() OVER (ORDER BY ORDINAL_POSITION) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :table_schema AND TABLE_NAME = :table_name
      MINUS
      SELECT COLUMN_NAME, DATA_TYPE, ROW_NUMBER() OVER (ORDER BY ORDINAL_POSITION) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :view_schema AND TABLE_NAME = :view_name
      )
  );

  -- Drop those columns first to avoid automatic re-creation of misordered columns
  if (columns_to_drop <> '') then
    SYSTEM$LOG_INFO('Will drop columns from ' || :table_schema || '.' || :table_name || ': ' || columns_to_drop);
    alter_table_drop_column := ' DROP ' || columns_to_drop;
    execute immediate alter_statement || alter_table_drop_column;
  end if;

  -- Super important that the columns to add are generated in the same order as the view. This ordering is pivotal for
  -- the migration algorithm to successfully function and make sure the multiple materialized views in
  -- INTERNAL_REPORTING_MV have the same schema (and can be UNION'ed together).
  -- We cannot, however, rely on the discrete ORDINAL_POSITION values because (due to previous bugs) we cannot guarantee
  -- that we have the same actual values for ORDINAL_POSITION. The goal after we drop and add columns is that the order
  -- of column_name are the same between the view and the table.
  let columns_to_add string := (
      -- we want to compare the columns in the ordering that ORDINAL_POSITION indicates, not the discrete values of ORDINAL_POSITION
      with view_columns as (
          SELECT COLUMN_NAME, DATA_TYPE, ROW_NUMBER() OVER (ORDER BY ORDINAL_POSITION) as rn FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :view_schema AND TABLE_NAME = :view_name
      ), table_columns as (
          SELECT COLUMN_NAME, DATA_TYPE, ROW_NUMBER() OVER (ORDER BY ORDINAL_POSITION) as rn FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :table_schema AND TABLE_NAME = :table_name
      ), cols_to_add as (
          select * from view_columns
            MINUS
          select * from table_columns
      ) SELECT LISTAGG('"' || COLUMN_NAME || '" ' || DATA_TYPE, ', ') within group (order by rn) from cols_to_add
  );

  let alter_table_add_column string := '';

  if (columns_to_add <> '') then
    SYSTEM$LOG_INFO('Will add columns to ' || :table_schema || '.' || :table_name || ': ' || columns_to_add);
    alter_table_add_column := ' ADD ' || columns_to_add;
    execute immediate alter_statement || alter_table_add_column;
  end if;

  if (columns_to_add <> '' OR columns_to_drop <> '') then
      SYSTEM$LOG_INFO('Migration executed for ' || :view_schema || '.' || :view_name || ' and ' || :table_schema || '.' || :table_name);
      SYSTEM$ADD_EVENT('table altered', {'alter_statement': alter_statement || alter_table_add_column || alter_table_drop_column });
      RETURN alter_table_add_column || alter_table_drop_column;
  else
    SYSTEM$LOG_INFO('No migration need for ' || :view_schema || '.' || :view_name || ' and ' || :table_schema || '.' || :table_name);
    RETURN 'no migration necessary';
  end if;
END;


CREATE OR REPLACE PROCEDURE internal.migrate_view()
    RETURNS STRING
    LANGUAGE SQL
    COMMENT = 'Re-create view used in the App. This is required when Snowflake adds a new column to query_history, or removes an existing column from query_history'
    AS
BEGIN
    call INTERNAL.create_view_QUERY_HISTORY_COMPLETE_AND_DAILY();
    call INTERNAL.create_view_enriched_query_history();
    call INTERNAL.create_view_enriched_query_history_daily();
    call INTERNAL.create_view_enriched_query_history_hourly();
    call INTERNAL.UPDATE_LABEL_VIEW();
    return 'Success';
END;

create or replace function internal.generate_column_names(source_schema varchar, source_table varchar)
returns string
as
$$
      -- Ordering the LISTAGG by ORDINAL_POSITION is not strictly necessary, but should eliminate confusion when LISTAGG would
      -- otherwise generate a random ordering of columns each time it is called.
      SELECT LISTAGG('"' || COLUMN_NAME || '"', ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
      FROM (
      SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = source_schema AND TABLE_NAME = source_table
      )
$$;


create or replace procedure internal.generate_insert_statement_cmd(target_schema varchar, target_table varchar, source_schema varchar, source_table varchar, where_clause varchar)
returns string
as
$$
begin
  let columns string := (select internal.generate_column_names(:source_schema, :source_table));

  let stmt string := 'INSERT INTO "' || :target_schema || '"."' || :target_table || '" (' || columns || ') SELECT ' || columns || ' FROM "' || :source_schema || '"."' || :source_table || '" where ' || :where_clause || ';';
  return :stmt;
end;
$$;

create or replace procedure internal.generate_insert_statement(target_schema varchar, target_table varchar, source_schema varchar, source_table varchar, where_clause varchar)
returns number
as
$$
begin
    let stmt varchar;
    call internal.generate_insert_statement_cmd(:target_schema, :target_table, :source_schema, :source_table, :where_clause) into stmt;
    SYSTEM$LOG_INFO('Running INSERT with generated query: ' || :stmt);
    execute immediate stmt;
    let inserted number := (select * from TABLE(RESULT_SCAN(LAST_QUERY_ID())));
  return :inserted;
end;
$$;

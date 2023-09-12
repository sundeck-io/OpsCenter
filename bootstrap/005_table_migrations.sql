
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

  let ctas_query string := '';
  let alter_query string := '';
  begin
      -- Does the view have more columns than the table?
      let table_missing_columns boolean := (select count(*) > 0 from (
          select column_name, data_type from information_schema.columns where table_schema = :view_schema and table_name = :view_name
          minus
          select column_name, data_type from information_schema.columns where table_schema = :table_schema and table_name = :table_name
      ));
      -- Does the table have more columns than the view?
      let view_missing_columns boolean := (select count(*) > 0 from (
          select column_name, data_type from information_schema.columns where table_schema = :table_schema and table_name = :table_name
          minus
          select column_name, data_type from information_schema.columns where table_schema = :view_schema and table_name = :view_name
      ));
      -- Are the list of columns (with ordinal position considered) the same for the view and the table?
      let has_misordered_columns boolean := (select count(*) > 0 from (
          select column_name, data_type, ordinal_position from information_schema.columns where table_schema = :view_schema and table_name = :view_name
          minus
          select column_name, data_type, ordinal_position from information_schema.columns where table_schema = :table_schema and table_name = :table_name
      ));

      -- In the unlikely event that the app has materialized the table with columns in the wrong order (as defined by ORDINAL_POSITION on the view) due
      -- to an earlier bug in OpsCenter, we need to recreate the tables in order for this migration logic to continue to function in the future.
      -- Snowflake does not support column reordering, so rewriting the data is the only alternative that doesn't introduce long-term debt maintaining
      -- a view to "correct" the data.
      --
      -- We try to detect this by asserting that the set of columns (name+type) for both the table and the view are the same but the ordering of those
      -- columns is different. Once we are sure that the set of columns is the same, we can naively check the order of those columns.
      if (:has_misordered_columns AND NOT :table_missing_columns AND NOT :view_missing_columns) THEN
          let column_spec string := (select internal.generate_column_def(:view_schema, :view_name));
          let view_columns string := (select internal.generate_column_names(:view_schema, :view_name));
          let swap_table_name string := :table_name || '_SWAP';

          begin
              begin transaction;
              execute immediate 'DROP TABLE IF EXISTS "' || :table_schema || '"."' || :swap_table_name || '"';
              -- CREATE TABLE "$schema"."$table_SWAP"($cols) AS SELECT $cols FROM "$schema"."$table"
              ctas_query := 'CREATE TABLE "' || :table_schema || '"."' || :swap_table_name || '" ( ' || :column_spec || ') AS SELECT ' || :view_columns || ' FROM "' || :table_schema || '"."' || :table_name || '"';
              execute immediate :ctas_query;
              -- ALTER TABLE "$schema"."$table" SWAP WITH "$schema"."$table_SWAP"
              alter_query := 'ALTER TABLE "' || :table_schema || '"."' || :table_name || '" SWAP WITH "' || :table_schema || '"."' || :swap_table_name || '"';
              execute immediate :alter_query;
              -- DROP TABLE "$schema"."$table_SWAP"
              execute immediate 'DROP TABLE "' || :table_schema || '"."' || :swap_table_name || '"';
              commit;
          exception
            when other then
              SYSTEM$LOG_ERROR(OBJECT_CONSTRUCT('error', 'Failed to migrate misordered table via swap', 'SQLCODE', :sqlcode, 'SQLERRM', :sqlerrm, 'SQLSTATE', :sqlstate));
              ROLLBACK;
              RAISE;
          end;

          SYSTEM$LOG_INFO('Table swap executed for ' || :view_schema || '.' || :view_name || ' and ' || :table_schema || '.' || :table_name);
          SYSTEM$ADD_EVENT('table swap migration', {'ctas_query': :ctas_query, 'alter_query': :alter_query });
      end if;
  end;

  -- Super important that the columns to add are consistently generated, else we may generate conflicting schemas between two tables created from the same view (the internal_reporting_mv query history tables)
  let columns_to_add string := (
      SELECT LISTAGG('"' || COLUMN_NAME || '" ' || DATA_TYPE, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
      FROM (
      SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :view_schema AND TABLE_NAME = :view_name
      MINUS
      SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :table_schema AND TABLE_NAME = :table_name
      )

      );

  -- Include ordinal_position to fix tables which have the correct columns but are in an unexpected order
  let columns_to_drop string := (
      SELECT LISTAGG('"' || COLUMN_NAME || '" ' , ', ')
      FROM (
      SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :table_schema AND TABLE_NAME = :table_name
      MINUS
      SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :view_schema AND TABLE_NAME = :view_name
      )

      );

  let alter_statement string := 'ALTER TABLE "' || :table_schema || '"."' || :table_name || '"';

  let alter_table_add_column string := '';
  let alter_table_drop_column string := '';

  -- Drop columns first, in case we are re-creating the same columns but in a different order.
  if (columns_to_drop <> '') then
    alter_table_drop_column := ' DROP ' || columns_to_drop;
    execute immediate alter_statement || alter_table_drop_column;
  end if;

  if (columns_to_add <> '') then
    alter_table_add_column := ' ADD ' || columns_to_add;
    execute immediate alter_statement || alter_table_add_column;
  end if;

  if (columns_to_add <> '' OR columns_to_drop <> '') then
      SYSTEM$LOG_INFO('Migration executed for ' || :view_schema || '.' || :view_name || ' and ' || :table_schema || '.' || :table_name);
      SYSTEM$ADD_EVENT('table altered', {'alter_statement': alter_statement || alter_table_add_column || alter_table_drop_column });
      let ctas_msg string := ' CTAS from swap migration ' || ctas_query;
      let alter_msg string := ' ALTER from swap migration ' || alter_query;
      RETURN alter_table_add_column || alter_table_drop_column || ctas_msg || alter_msg;
  else
    SYSTEM$LOG_INFO('No migration need for ' || :view_schema || '.' || :view_name || ' and ' || :table_schema || '.' || :table_name);
    RETURN null;
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

create or replace function internal.generate_column_def(source_schema varchar, source_table varchar)
returns string
as
$$
      -- Ordering the LISTAGG by ORDINAL_POSITION is not strictly necessary, but should eliminate confusion when LISTAGG would
      -- otherwise generate a random ordering of columns each time it is called.
      SELECT LISTAGG('"' || COLUMN_NAME || '" ' || DATA_TYPE, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
      FROM (
      SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = source_schema AND TABLE_NAME = source_table
      )
$$;


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

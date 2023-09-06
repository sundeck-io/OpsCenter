
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

  let columns_to_add string := (
      SELECT LISTAGG('"' || COLUMN_NAME || '" ' || DATA_TYPE, ', ')
      FROM (
      SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :view_schema AND TABLE_NAME = :view_name
      MINUS
      SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :table_schema AND TABLE_NAME = :table_name
      )

      );

  let columns_to_drop string := (
      SELECT LISTAGG('"' || COLUMN_NAME || '" ' , ', ')
      FROM (
      SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :table_schema AND TABLE_NAME = :table_name
      MINUS
      SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :view_schema AND TABLE_NAME = :view_name
      )

      );

  let alter_statement string := 'ALTER TABLE "' || :table_schema || '"."' || :table_name || '"';

  let alter_table_add_column string := '';
  let alter_table_drop_column string := '';

  if (columns_to_add <> '') then
    alter_table_add_column := ' ADD ' || columns_to_add;
    execute immediate alter_statement || alter_table_add_column;
  end if;

  if (columns_to_drop <> '') then
    alter_table_drop_column := ' DROP ' || columns_to_drop;
    execute immediate alter_statement || alter_table_drop_column;
  end if;

  if (columns_to_add <> '' OR columns_to_drop <> '') then
      SYSTEM$LOG_INFO('Migration executed for ' || :view_schema || '.' || :view_name || ' and ' || :table_schema || '.' || :table_name);
      SYSTEM$ADD_EVENT('table altered', {'alter_statement': alter_statement || alter_table_add_column || alter_table_drop_column });
      RETURN alter_table_add_column || alter_table_drop_column;
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

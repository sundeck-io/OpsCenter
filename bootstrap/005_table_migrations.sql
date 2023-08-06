
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

--  let missing_view_columns boolean := (
--      select count(*) > 0 FROM (
--      SELECT COLUMN_NAME, DATA_TYPE  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :table_schema AND TABLE_NAME = :table_name
--      MINUS
--      SELECT COLUMN_NAME, DATA_TYPE  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :view_schema AND TABLE_NAME = :view_name
--      )
--      );
--  IF (missing_view_columns) THEN
--      SYSTEM$ADD_EVENT('table out of sync - columns exist in table distinct from those in view', {'count_missing': (:missing_view_columns)});
--      RAISE OUT_OF_SYNC;
--  END IF;


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

  let alter_statement_detail string := '';

  if (columns_to_add <> '') then
    alter_statement_detail := alter_statement_detail || ' ADD ' || columns_to_add;
  end if;
  if (columns_to_drop <> '') then
    alter_statement_detail := alter_statement_detail || ' DROP ' || columns_to_drop;
  end if;

  if (columns_to_add <> '' OR columns_to_drop <> '') then
      alter_statement := alter_statement || alter_statement_detail;
      execute immediate alter_statement;
      SYSTEM$LOG_INFO('Migration executed for ' || :view_schema || '.' || :view_name || ' and ' || :table_schema || '.' || :table_name);
      SYSTEM$ADD_EVENT('table altered', {'alter_statement': alter_statement });
      RETURN alter_statement_detail;
  else
    SYSTEM$LOG_INFO('No migration need for ' || :view_schema || '.' || :view_name || ' and ' || :table_schema || '.' || :table_name);
    RETURN null;
  end if;
END;

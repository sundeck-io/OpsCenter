
CREATE SCHEMA IF NOT EXISTS INTERNAL;

CREATE TABLE INTERNAL.CONFIG IF NOT EXISTS (KEY TEXT, VALUE TEXT);

CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_CONFIG_TABLE()
RETURNS OBJECT
AS
BEGIN
    -- Add PRIMARY KEY
    IF (NOT EXISTS(select * from information_schema.table_constraints where TABLE_SCHEMA = 'INTERNAL' AND table_name = 'CONFIG'  AND CONSTRAINT_TYPE = 'PRIMARY KEY')) THEN
        ALTER TABLE INTERNAL.CONFIG ADD CONSTRAINT CONFIG_PK PRIMARY KEY (KEY);
    END IF;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'Failed to migrate CONFIG table. ' || :SQLCODE || ': ' || :SQLERRM);
        raise;
END;

CALL INTERNAL.MIGRATE_CONFIG_TABLE();

CREATE OR REPLACE PROCEDURE internal.set_config(key string, value string) RETURNS STRING LANGUAGE SQL
    AS
BEGIN
    MERGE INTO internal.config AS target
    USING (SELECT :key AS key, :value AS value
    ) AS source
    ON target.key = source.key
    WHEN MATCHED THEN
      UPDATE SET value = source.value
    WHEN NOT MATCHED THEN
      INSERT (key, value) VALUES (source.key, source.value);
END;

CREATE OR REPLACE PROCEDURE internal.get_config(key string) RETURNS STRING LANGUAGE SQL
    AS
BEGIN
    let config_value string := (SELECT value FROM internal.config WHERE key = :key);
    return config_value;
END;

CREATE OR REPLACE PROCEDURE internal.has_config(key string) RETURNS BOOLEAN LANGUAGE SQL
    AS
BEGIN
    let config_value string := (SELECT value FROM internal.config WHERE key = :key);
    return (select :config_value is not null);
END;

create or replace function internal.get_version() returns string language sql
as
$$
'{{ git_hash }}'
$$;

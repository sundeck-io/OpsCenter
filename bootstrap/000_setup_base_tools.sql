
CREATE SCHEMA IF NOT EXISTS INTERNAL;

CREATE TABLE INTERNAL.CONFIG IF NOT EXISTS (KEY TEXT, VALUE TEXT);

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
DECLARE
    EXCEPTION_DUP_KEYS EXCEPTION(-20001, 'More than 1 rows in table internal.config for the specified key');
BEGIN
    let cnt number := (SELECT COUNT(*) AS cnt FROM internal.config WHERE key = :key);

    IF (cnt = 0) THEN
        return NULL;
    ELSEIF (cnt > 1) THEN
        RAISE EXCEPTION_DUP_KEYS;
    END IF;

    let config_value string := (SELECT value FROM internal.config WHERE key = :key);
    return config_value;
END;

create or replace function internal.get_version() returns string language sql
as
$$
'{{ git_hash }}'
$$;


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

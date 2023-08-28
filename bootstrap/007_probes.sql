
CREATE TABLE INTERNAL.PROBES IF NOT EXISTS (name string, condition string, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean, enabled boolean, probe_modified_at timestamp, probe_created_at timestamp);
CREATE OR REPLACE VIEW CATALOG.PROBES AS SELECT * FROM INTERNAL.PROBES;

CREATE TABLE INTERNAL.PREDEFINED_PROBES if not exists (name string, condition string, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean, enabled boolean, probe_modified_at timestamp, probe_created_at timestamp);

CREATE TABLE INTERNAL.PROBE_ACTIONS (action_time timestamp, probe_name string, query_id string, actions_taken variant, outcome string) IF NOT EXISTS;
CREATE OR REPLACE VIEW REPORTING.PROBE_ACTIONS AS SELECT * FROM INTERNAL.PROBE_ACTIONS;

CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_PROBES_TABLE()
RETURNS OBJECT
AS
BEGIN
    -- Add NOTIFY_WRITER_METHOD column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PROBES' AND COLUMN_NAME = 'NOTIFY_WRITER_METHOD')) THEN
        ALTER TABLE IF EXISTS INTERNAL.PROBES ADD COLUMN NOTIFY_WRITER_METHOD STRING DEFAULT 'EMAIL';
    END IF;

    -- Add NOTIFY_OTHER_METHOD column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PROBES' AND COLUMN_NAME = 'NOTIFY_OTHER_METHOD')) THEN
        ALTER TABLE IF EXISTS INTERNAL.PROBES ADD COLUMN NOTIFY_OTHER_METHOD STRING DEFAULT 'EMAIL';
    END IF;

    -- Rename EMAIL_WRITER to NOTIFY_WRITER
    IF (EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PROBES' AND COLUMN_NAME = 'EMAIL_WRITER')) THEN
        ALTER TABLE IF EXISTS INTERNAL.PROBES RENAME COLUMN EMAIL_WRITER to NOTIFY_WRITER;
    END IF;

    -- Rename EMAIL_OTHER to NOTIFY_OTHER
    IF (EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PROBES' AND COLUMN_NAME = 'EMAIL_OTHER')) THEN
        ALTER TABLE IF EXISTS INTERNAL.PROBES RENAME COLUMN EMAIL_OTHER to NOTIFY_OTHER;
    END IF;
    -- Add modified at column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PROBES' AND COLUMN_NAME = 'PROBE_MODIFIED_AT')) THEN
        ALTER TABLE INTERNAL.PROBES ADD COLUMN PROBE_MODIFIED_AT TIMESTAMP;
        UPDATE INTERNAL.PROBES SET PROBE_MODIFIED_AT = CURRENT_TIMESTAMP() WHERE PROBE_MODIFIED_AT IS NULL;
    END IF;

    -- Add created at column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PROBES' AND COLUMN_NAME = 'PROBE_CREATED_AT')) THEN
        ALTER TABLE INTERNAL.PROBES ADD COLUMN PROBE_CREATED_AT TIMESTAMP;
        UPDATE INTERNAL.PROBES SET PROBE_CREATED_AT = CURRENT_TIMESTAMP() WHERE PROBE_CREATED_AT IS NULL;
    END IF;

    -- Recreate the view to avoid number of column mis-match. Should be cheap and only run on install/upgrade, so it's OK
    -- if we run this unnecessarily.
    CREATE OR REPLACE VIEW CATALOG.PROBES COPY GRANTS AS SELECT * FROM INTERNAL.PROBES;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'Failed to migrate probes table. ' || :SQLCODE || ': ' || :SQLERRM);
        raise;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_PREDEFINED_PROBES_TABLE()
RETURNS OBJECT
AS
BEGIN
    -- Add modified at column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PREDEFINED_PROBES' AND COLUMN_NAME = 'PROBE_MODIFIED_AT')) THEN
        ALTER TABLE INTERNAL.PREDEFINED_PROBES ADD COLUMN PROBE_MODIFIED_AT TIMESTAMP;
        UPDATE INTERNAL.PREDEFINED_PROBES SET PROBE_MODIFIED_AT = CURRENT_TIMESTAMP() WHERE PROBE_MODIFIED_AT IS NULL;
    END IF;

    -- Add created at column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PREDEFINED_PROBES' AND COLUMN_NAME = 'PROBE_CREATED_AT')) THEN
        ALTER TABLE INTERNAL.PREDEFINED_PROBES ADD COLUMN PROBE_CREATED_AT TIMESTAMP;
        UPDATE INTERNAL.PREDEFINED_PROBES SET PROBE_CREATED_AT = CURRENT_TIMESTAMP() WHERE PROBE_CREATED_AT IS NULL;
    END IF;

EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'Failed to migrate predefined_probes table. ' || :SQLCODE || ': ' || :SQLERRM);
        raise;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.VALIDATE_PROBE_CONDITION(name string, condition string)
RETURNS STRING
AS
BEGIN
    let statement string := 'select case when \n' || condition || '\n then 1 else 0 end as "' || name || '" from INTERNAL.DUMMY_QUERY_HISTORY_UDTF';
    execute immediate statement;
    return null;
EXCEPTION
    when statement_error then
        return 'Invalid condition SQL. Please check your syntax.' || :SQLERRM;
    WHEN OTHER THEN
        return 'Failure validating name & condition. Please check your syntax.' || :SQLERRM;
END;


CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_PROBE_MONITOR_RUNNING()
    RETURNS string
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    let probes_enabled boolean := (select count(*) > 0 from internal.probes where cancel or notify_writer or length(notify_other) > 3);
    let configured boolean := (select count(*) > 0 from internal.config where key = 'post_setup');
    if (probes_enabled and configured) then
        execute immediate 'alter task tasks.PROBE_MONITORING resume';
    else
        execute immediate 'alter task tasks.PROBE_MONITORING suspend';
    end if;
END;

CREATE OR REPLACE FUNCTION INTERNAL.MERGE_CONCAT_OBJECTS(O1 VARIANT, O2 VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    let result = {};
    for (let key in O1) {
        result[key] = O1[key];
    }
    for (let key in O2) {
        if (result[key]) {
            result[key] += ',' + O2[key];
        } else {
            result[key] = O2[key];
        }
    }
    return result;
$$
;

CREATE OR REPLACE FUNCTION INTERNAL.MERGE_OBJECTS(O1 VARIANT, O2 VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS 'return Object.assign(O1, O2);'
;

CREATE OR REPLACE PROCEDURE INTERNAL.GET_PROBE_SELECT()
RETURNS string
AS
BEGIN
    -- TODO: find a way to improve this so we're not double querying probes.
    -- We do it here so we don't have to transport objects from queries to sql text but it means
    -- there is a slight chance of seeing different actions for the probe between the first step and the second.
    let s string := $$
    with
    users as (
        select name, email from internal.sfusers
    ),
    probes as (
        select * from internal.probes where cancel or notify_writer or length(notify_other) > 3
    ),
    actions as (
    SELECT current_timestamp() as probe_time, query_id, user_name, query_text, warehouse_name, start_time, case $$;
    let found boolean := false;
    let probes cursor for select name, condition from internal.probes;
    for probe in probes do
        found := true;
        s := s || '\n\t when ' || probe.condition || $$ then '$$ || probe.name || $$' $$;
    end for;

    if (not found) then
        s := s || '\n\t when true then null::text ';
    end if;
    s := s || $$
    else null end as probe_to_execute
    from table(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(CURRENT_TIMESTAMP()))
    where session_id <> current_session()
    ),
    items as (
    select
        probe_time,
        probe_to_execute as probe_name, query_id,
        case when p.notify_writer and u.email is not null then OBJECT_CONSTRUCT(p.notify_writer_method, u.email) else OBJECT_CONSTRUCT() end as notify_writer_obj,
        case when p.notify_other is not null then OBJECT_CONSTRUCT(p.notify_other_method, p.notify_other) else OBJECT_CONSTRUCT() end as notify_other_obj,
        OBJECT_INSERT(INTERNAL.MERGE_CONCAT_OBJECTS(notify_writer_obj, notify_other_obj), 'CANCEL', p.cancel) as action_taken,
        a.user_name,
        a.warehouse_name,
        a.start_time,
        a.query_text
    from actions a
    join probes p on a.probe_to_execute = p.name
    left join users u on a.user_name = u.name
    where probe_to_execute is not null
    and (probe_to_execute, query_id) not in (select probe_name, query_id from internal.probe_actions)
    )
    select probe_time, probe_name, query_id, action_taken, user_name, warehouse_name, start_time, query_text from items
    $$;
    return s;
END;

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_PROBE(name text, condition text, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    if (name is null) then
      return 'Name must not be null.';
    elseif (condition is null) then
      return 'Condition must not be null.';
    end if;
    let outcome text := '';
    outcome := (CALL INTERNAL.VALIDATE_PROBE_CONDITION(:name, :condition));

    if (outcome is not null) then
      return outcome;
    end if;

    BEGIN TRANSACTION;
        let cnt number := (SELECT COUNT(*) AS cnt FROM internal.probes WHERE name = :name);

        IF (cnt > 0) THEN
            outcome := 'A probe with this name already exists. Please choose a distinct name.';
        ELSE
          INSERT INTO internal.probes ("NAME", "CONDITION", "NOTIFY_WRITER", "NOTIFY_WRITER_METHOD", "NOTIFY_OTHER", "NOTIFY_OTHER_METHOD", "CANCEL", "PROBE_MODIFIED_AT")
            VALUES (:name, :condition, :notify_writer, :notify_writer_method, :notify_other, :notify_other_method, :cancel, current_timestamp());
          outcome := null;
        END IF;

    COMMIT;
    call ADMIN.UPDATE_PROBE_MONITOR_RUNNING();
    return outcome;
END;

CREATE OR REPLACE PROCEDURE ADMIN.DELETE_PROBE(name text)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    if (:name is null) then
      return 'Name must not be null.';
    end if;

    DELETE FROM internal.probes where name = :name;
    call ADMIN.UPDATE_PROBE_MONITOR_RUNNING();
    return 'done';
END;

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_PROBE(oldname text, name text, condition text, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    if (name is null) then
      return 'Name must not be null.';
    elseif (condition is null) then
      return 'Condition must not be null.';
    end if;

    let outcome text := '';
    outcome := (CALL INTERNAL.VALIDATE_PROBE_CONDITION(:name, :condition));

    if (outcome is not null) then
      return outcome;
    end if;

    BEGIN TRANSACTION;

    -- Make sure that the old name exists once and the new name doesn't exist (assuming it is different from the old name)
    let oldcnt number := (SELECT COUNT(*) AS cnt FROM internal.probes WHERE name = :oldname);
    let newcnt number := (SELECT COUNT(*) AS cnt FROM internal.probes WHERE name = :name AND name <> :oldname);

    IF (oldcnt <> 1) THEN
      outcome := 'Probe not found. Please refresh your page to see latest list of probes.';
    ELSEIF (newcnt <> 0) THEN
      outcome := 'A probe with this name already exists. Please choose a distinct name.';
    ELSE
      UPDATE internal.probes SET NAME = :name,
                                 NOTIFY_WRITER = :notify_writer, NOTIFY_WRITER_METHOD = :notify_writer_method,
                                 NOTIFY_OTHER = :notify_other, NOTIFY_OTHER_METHOD = :notify_other_method,
                                 CANCEL = :cancel,
                                 CONDITION = :condition,
                                 PROBE_MODIFIED_AT = current_timestamp()
                             WHERE NAME = :oldname;
      outcome := null;
    END IF;

    COMMIT;
    call ADMIN.UPDATE_PROBE_MONITOR_RUNNING();
    return outcome;
EXCEPTION
  WHEN OTHER THEN
      ROLLBACK;
      RAISE;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.POPULATE_PREDEFINED_PROBES()
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    MERGE INTO internal.predefined_probes t
    USING (
        SELECT *
        from (values
                ('Long Queries', 'start_time < dateadd(minute, -10, current_timestamp())'),
                ('Big Readers', 'bytes_scanned > 10000000000')
             )) s (name, condition)
    ON t.name = s.name
    WHEN MATCHED THEN
    UPDATE
        SET t.CONDITION = s.CONDITION, t.PROBE_MODIFIED_AT = current_timestamp()
    WHEN NOT MATCHED THEN
    INSERT
        ("NAME", "CONDITION", "NOTIFY_WRITER", "NOTIFY_WRITER_METHOD", "NOTIFY_OTHER", "NOTIFY_OTHER_METHOD", "CANCEL", "PROBE_MODIFIED_AT", "PROBE_CREATED_AT")
        VALUES (s.name, s.condition, False,  'Email', '', 'Email', False, current_timestamp(), current_timestamp());

    RETURN NULL;
EXCEPTION
  WHEN OTHER THEN
      ROLLBACK;
      RAISE;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.INITIALIZE_PROBES()
    RETURNS boolean
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    let probecnt number := (SELECT COUNT(*) FROM internal.probes);

    let probe_inited text := '';
    probe_inited := (CALL INTERNAL.get_config('PROBES_INITED'));

    if (probecnt > 0 OR probe_inited = 'True') THEN
        SYSTEM$LOG_INFO('Predefined probes import is skipped. \n');
        RETURN FALSE;
    ELSE
        INSERT INTO INTERNAL.PROBES ("NAME", "CONDITION", "NOTIFY_WRITER", "NOTIFY_WRITER_METHOD", "NOTIFY_OTHER", "NOTIFY_OTHER_METHOD", "CANCEL", "PROBE_MODIFIED_AT", "PROBE_CREATED_AT")
            SELECT "NAME", "CONDITION", "NOTIFY_WRITER", "NOTIFY_WRITER_METHOD", "NOTIFY_OTHER", "NOTIFY_OTHER_METHOD", "CANCEL", "PROBE_CREATED_AT", "PROBE_CREATED_AT"
            FROM INTERNAL.PREDEFINED_PROBES;
        CALL INTERNAL.SET_CONFIG('PROBES_INITED', 'True');
        SYSTEM$LOG_INFO('Predefined probes are imported into PROBES table. \n');
        RETURN TRUE;
    END IF;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_PREDEFINED_PROBES(gap_in_seconds NUMBER)
    RETURNS BOOLEAN
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
BEGIN
    let rowCount1 number := (
        WITH
        OLD_PREDEFINED_PROBES AS
            (SELECT name, PROBE_CREATED_AT FROM INTERNAL.PREDEFINED_PROBES WHERE TIMESTAMPDIFF(SECOND, PROBE_CREATED_AT, CURRENT_TIMESTAMP) > :gap_in_seconds),
        USER_PROBES AS
            (SELECT name, PROBE_MODIFIED_AT FROM INTERNAL.PROBES)
        SELECT count(*) from (select * from OLD_PREDEFINED_PROBES MINUS SELECT * FROM USER_PROBES) S
        );
    let rowCount2 number := (
        WITH
        OLD_PREDEFINED_PROBES AS
            (SELECT name, PROBE_CREATED_AT FROM INTERNAL.PREDEFINED_PROBES WHERE TIMESTAMPDIFF(SECOND, PROBE_CREATED_AT, CURRENT_TIMESTAMP) > :gap_in_seconds),
        USER_PROBES AS
            (SELECT name, PROBE_MODIFIED_AT FROM INTERNAL.PROBES)
        SELECT count(*) from (select * from  USER_PROBES MINUS SELECT * FROM OLD_PREDEFINED_PROBES ) S
        );

    IF (rowCount1 > 0 OR rowCount2 > 0) THEN
        RETURN FALSE;
    END IF;

    MERGE INTO internal.probes t
    USING internal.predefined_probes s
    ON t.name = s.name
    WHEN MATCHED THEN
    UPDATE
        SET t.CONDITION = s.CONDITION,
            t.NOTIFY_WRITER = s.NOTIFY_WRITER,
            t.NOTIFY_WRITER_METHOD = s.NOTIFY_WRITER_METHOD,
            t.NOTIFY_OTHER = s.NOTIFY_OTHER,
            t.NOTIFY_OTHER_METHOD = s.NOTIFY_OTHER_METHOD,
            t.CANCEL = s.CANCEL,
            t.PROBE_MODIFIED_AT = s.PROBE_CREATED_AT
    WHEN NOT MATCHED THEN
    INSERT ("NAME", "CONDITION", "NOTIFY_WRITER", "NOTIFY_WRITER_METHOD", "NOTIFY_OTHER", "NOTIFY_OTHER_METHOD", "CANCEL", "PROBE_MODIFIED_AT", "PROBE_CREATED_AT")
        VALUES (s.NAME, s."CONDITION", s."NOTIFY_WRITER", s."NOTIFY_WRITER_METHOD", s."NOTIFY_OTHER", s."NOTIFY_OTHER_METHOD", s."CANCEL", s."PROBE_CREATED_AT", s."PROBE_CREATED_AT");

    return TRUE;
END;
$$;

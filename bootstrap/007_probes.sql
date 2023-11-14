
CREATE TABLE INTERNAL.PROBES IF NOT EXISTS (name string, condition string, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean, enabled boolean, probe_modified_at timestamp, probe_created_at timestamp);
CREATE OR REPLACE VIEW CATALOG.PROBES AS SELECT * FROM INTERNAL.PROBES;
CREATE OR REPLACE VIEW CATALOG.QUERY_MONITORS AS SELECT * FROM INTERNAL.PROBES;

CREATE TABLE INTERNAL.PREDEFINED_PROBES if not exists (name string, condition string, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean, enabled boolean, probe_modified_at timestamp, probe_created_at timestamp);

CREATE TABLE INTERNAL.PROBE_ACTIONS (action_time timestamp, probe_name string, query_id string, actions_taken variant, outcome string) IF NOT EXISTS;
CREATE OR REPLACE VIEW REPORTING.PROBE_ACTIONS AS SELECT * FROM INTERNAL.PROBE_ACTIONS;
CREATE OR REPLACE VIEW REPORTING.QUERY_MONITOR_ACTIVITY AS SELECT * FROM INTERNAL.PROBE_ACTIONS;

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

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_PROBE_MONITOR_RUNNING()
    RETURNS string
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    let cancel_probes_enabled boolean := (select count(*) > 0 from internal.probes where cancel);
    let notify_probes_enabled boolean := (select count(*) > 0 from internal.probes where notify_writer or length(notify_other) > 3);
    let notify_setup boolean := (select count(*) > 0 from internal.config where key in ('url','tenant_url'));
    let configured boolean := (select count(*) > 0 from internal.config where key = 'post_setup');
    if (cancel_probes_enabled and configured) then
        execute immediate 'alter task tasks.PROBE_MONITORING resume';
    elseif (notify_probes_enabled and notify_setup and configured) then
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
    --
    -- The actions CTE is doing two-levels of filtering to ensure each probe fires exactly one per matched query.
    -- The addition clauses on top of the CONDITION in the case statements ensure probes with duplicate conditions
    -- will each match. The final NOT IN clause of the where condition for the CTE will ensure that we don't re-trigger
    -- the same probe again for the same query when multiple probes exist with the same condition.
    let s string := $$
    with
    users as (
        select name, email from internal.sfusers
    ),
    probes as (
        select * from internal.probes where cancel or notify_writer or length(notify_other) > 3
    ),
    actions as (
    SELECT current_timestamp() as probe_time, qh.query_id, user_name, query_text, warehouse_name, start_time, case $$;
    let found boolean := false;
    let probes cursor for select name, condition from internal.probes;
    for probe in probes do
        found := true;
        s := s || '\n\t when ' || probe.condition || $$ and (actions.probe_name != '$$ || probe.name || $$' or actions.probe_name is null) then '$$ || probe.name || $$' $$;
    end for;

    if (not found) then
        s := s || '\n\t when true then null::text ';
    end if;
    s := s || $$
    else null end as probe_to_execute
    from table(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(CURRENT_TIMESTAMP())) as qh
    left outer join internal.probe_actions as actions on qh.query_id = actions.query_id
    where session_id <> current_session() and
        (probe_to_execute, qh.query_id) not in (select probe_name, query_id from internal.probe_actions)
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
    )
    select probe_time, probe_name, query_id, action_taken, user_name, warehouse_name, start_time, query_text from items
    $$;
    return s;
END;

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_PROBE(name text, condition text, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'create_probe'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
import datetime
from crud import create_entity
def create_probe(session, name, condition, notify_writer, notify_writer_method, notify_other, notify_other_method, cancel):
    return create_entity(session, 'PROBE', {'name': name, 'condition': condition, 'notify_writer': notify_writer, 'notify_writer_method': notify_writer_method, 'notify_other': notify_other, 'notify_other_method': notify_other_method, 'cancel': cancel, 'probe_created_at': datetime.datetime.now(), 'probe_modified_at': datetime.datetime.now()})
$$;


CREATE OR REPLACE PROCEDURE ADMIN.DELETE_PROBE(name text)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'delete_probe'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
from crud import delete_entity
def delete_probe(session, name):
    return delete_entity(session, 'PROBE', name)
$$;


CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_PROBE(oldname text, name text, condition text, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean)
    RETURNS TEXT
    LANGUAGE PYTHON
    runtime_version = "3.10"
    handler = 'update_probe'
    packages = ('snowflake-snowpark-python', 'pydantic')
    imports = ('{{stage}}/python/crud.zip')
    EXECUTE AS OWNER
AS
$$
import datetime
from crud import update_entity
def update_probe(session, oldname, name, condition, notify_writer, notify_writer_method, notify_other, notify_other_method, cancel):
    return update_entity(session, 'PROBE', oldname, {'name': name, 'condition': condition, 'notify_writer': notify_writer, 'notify_writer_method': notify_writer_method, 'notify_other': notify_other, 'notify_other_method': notify_other_method, 'cancel': cancel, 'probe_created_at': datetime.datetime.now(), 'probe_modified_at': datetime.datetime.now()})
$$;


CREATE OR REPLACE PROCEDURE ADMIN.CREATE_QUERY_MONITOR(name text, condition text, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean)
RETURNS TEXT
LANGUAGE SQL
AS $$
begin
let retval varchar;
call admin.create_probe(:name, :condition, :notify_writer, :notify_writer_method, :notify_other, :notify_other_method, :cancel) into :retval;
return :retval;
end;
$$;

CREATE OR REPLACE PROCEDURE ADMIN.DELETE_QUERY_MONITOR(name text)
RETURNS TEXT
LANGUAGE SQL
AS $$
begin
let retval varchar;
call admin.delete_probe(:name) into :retval;
return :retval;
end;
$$;


CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_QUERY_MONITOR(oldname text, name text, condition text, notify_writer boolean, notify_writer_method string, notify_other string, notify_other_method string, cancel boolean)
RETURNS TEXT
LANGUAGE SQL
AS $$
begin
let retval varchar;
call admin.update_probe(:oldname, :name, :condition, :notify_writer, :notify_writer_method, :notify_other, :notify_other_method, :cancel) into :retval;
return :retval;
end;
$$;

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
                ('Long Queries', 'start_time < dateadd(minute, -10, current_timestamp()) AND NOT QUERY_TYPE = \'EXECUTE_STREAMLIT\'', False),
                ('Big Readers', 'bytes_scanned > 10000000000', False),
                ('Costs 10 Credits', 'tools.approx_credits_used(warehouse_name, start_time) > 10', True),
                ('Costs 50 Credits', 'tools.approx_credits_used(warehouse_name, start_time) > 50', True)
             )) s (name, condition, notify_writer)
    ON t.name = s.name
    WHEN MATCHED THEN
    UPDATE
        SET t.CONDITION = s.CONDITION, t.PROBE_MODIFIED_AT = current_timestamp(), t.NOTIFY_WRITER = s.NOTIFY_WRITER
    WHEN NOT MATCHED THEN
    INSERT
        ("NAME", "CONDITION", "NOTIFY_WRITER", "NOTIFY_WRITER_METHOD", "NOTIFY_OTHER", "NOTIFY_OTHER_METHOD", "CANCEL", "PROBE_MODIFIED_AT", "PROBE_CREATED_AT")
        VALUES (s.name, s.condition, s.notify_writer,  'Email', '', 'Email', False, current_timestamp(), current_timestamp());

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

CREATE OR REPLACE PROCEDURE INTERNAL.MERGE_PREDEFINED_PROBES()
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
  INSERT INTO internal.probes  (name, condition, notify_writer, notify_writer_method, notify_other, notify_other_method, cancel, probe_modified_at, probe_created_at) select name, condition, notify_writer, notify_writer_method, notify_other, notify_other_method, cancel, probe_modified_at, probe_created_at from internal.predefined_probes s where s.name not in (select name from internal.probes);
$$;

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

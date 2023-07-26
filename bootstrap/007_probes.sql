
CREATE TABLE INTERNAL.PROBES (name string, condition string, email_writer boolean, email_other string, cancel boolean, enabled boolean) IF NOT EXISTS;
CREATE OR REPLACE VIEW CATALOG.PROBES AS SELECT * FROM INTERNAL.PROBES;

CREATE TABLE INTERNAL.PROBE_ACTIONS (action_time timestamp, probe_name string, query_id string, actions_taken variant, outcome string) IF NOT EXISTS;
CREATE OR REPLACE VIEW REPORTING.PROBE_ACTIONS AS SELECT * FROM INTERNAL.PROBE_ACTIONS;

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
    let enable boolean := (select count(*) > 0 from internal.probes where cancel or email_writer or length(email_other) > 3);
    let configured boolean := (select count(*) > 0 from internal.config where key = 'post_setup');
    if (enable and configured) then
        execute immediate 'alter task tasks.PROBE_MONITORING resume';
    else
        execute immediate 'alter task tasks.PROBE_MONITORING suspend';
    end if;
END;

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
        select * from internal.probes where cancel or email_writer or length(email_other) > 3
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
        case when p.email_writer and u.email is not null then u.email else '' end as uemail,
        case when p.email_other is not null then p.email_other else '' end as oemail,
        OBJECT_CONSTRUCT('EMAIL', uemail || ',' || oemail, 'CANCEL', p.cancel) as action,
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
    select probe_time, probe_name, query_id, action as action_taken, user_name, warehouse_name, start_time, query_text from items
    $$;
    return s;
END;

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_PROBE(name text, condition text, email_writer boolean, email_other string, cancel boolean)
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
          INSERT INTO internal.probes ("NAME", "CONDITION", "EMAIL_WRITER", "EMAIL_OTHER", "CANCEL") VALUES (:name, :condition, :email_writer, :email_other, :cancel);
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

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_PROBE(oldname text, name text, condition text, email_writer boolean, email_other string, cancel boolean)
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
      UPDATE internal.probes SET  NAME = :name, EMAIL_WRITER = :email_writer, EMAIL_OTHER = :email_other, CANCEL = :cancel, CONDITION = :condition WHERE NAME = :oldname;
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

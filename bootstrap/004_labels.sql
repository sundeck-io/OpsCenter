
CREATE TABLE INTERNAL.LABELS if not exists (name string, group_name string null, group_rank number, label_created_at timestamp, condition string, enabled boolean);

CREATE OR REPLACE PROCEDURE INTERNAL.VALIDATE_LABEL_CONDITION(name string, condition string)
RETURNS STRING
AS
BEGIN
    let statement string := 'select case when \n' || condition || '\n then 1 else 0 end as "' || name || '" from reporting.enriched_query_history where false';
    execute immediate statement;
    return null;
EXCEPTION
    when statement_error then
        return 'Invalid condition SQL. Please check your syntax.' || :SQLERRM;
    WHEN OTHER THEN
        return 'Failure validating name & condition. Please check your syntax.' || :SQLERRM;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.UPDATE_LABEL_VIEW()
RETURNS boolean
AS
BEGIN
    let labels cursor for select name, condition from internal.labels where group_name is null;
    let s string := $$
CREATE OR REPLACE VIEW REPORTING.LABELED_QUERY_HISTORY
AS
SELECT *,$$;
    for label in labels do
         s := s || '\n\tcase when ' || label.condition || ' then true else false end as "' || label.name || '",';
    end for;

    let grouped_labels cursor for select group_name, name, condition from internal.labels where group_name is not null order by group_name, group_rank;
    let group_name string := null;
    for grp in grouped_labels do
        if (grp.group_name <> group_name or group_name is null) then
            if (group_name is not null) then
                s := s || ' end as "'|| group_name || '", ';
            end if;

            s := s || '\n\tcase ';
        end if;

        s := s || ' when ' || grp.condition || $$ then '$$ || grp.name || $$' $$;
        group_name := grp.group_name;
    end for;

    if (group_name is not null) then
        s := s || $$ else 'Other' end as "$$ || group_name || '", ';
    end if;


    s := s || '\n\t1 as not_used_internal\nFROM REPORTING.ENRICHED_QUERY_HISTORY';
    SYSTEM$LOG_INFO('Updating label definitions. Updated SQL: \n' || s);
    --return s;
    execute immediate s;
    return true;
END;

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_LABEL(name text, grp text, rank number, condition text)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    if (:name is null) then
      return 'Name must not be null.';
    elseif (grp is null and rank is not null) then
      return 'Rank must only be set if Group name is also provided.';
    elseif (grp is not null and rank is null) then
      return 'Rank must provided if you are creating a grouped label.';
    end if;
    let outcome text := 'Failure validating name & condition. Please check your syntax.';

    outcome := (CALL INTERNAL.VALIDATE_LABEL_CONDITION(:name, :condition));

    if (outcome is not null) then
      return outcome;
    end if;

    outcome := 'Duplicate label name found. Please use a distinct name.';
    BEGIN TRANSACTION;
        let cnt number := (SELECT COUNT(*) AS cnt FROM internal.labels WHERE name = :name);

        IF (cnt = 0) THEN
          INSERT INTO internal.labels ("NAME", "GROUP_NAME", "GROUP_RANK", "LABEL_CREATED_AT", "CONDITION") VALUES (:name, :grp, :rank, current_timestamp(), :condition);
          outcome := null;
        END IF;

    COMMIT;
    CALL INTERNAL.UPDATE_LABEL_VIEW();
    return outcome;

END;


CREATE OR REPLACE PROCEDURE ADMIN.DELETE_LABEL(name text)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    if (:name is null) then
      return 'Name must not be null.';
    end if;

    DELETE FROM internal.labels where name = :name;
    CALL INTERNAL.UPDATE_LABEL_VIEW();
    return 'done';
END;

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_LABEL(oldname text, name text, grp text, rank number, condition text)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    if (name is null) then
      return 'Name must not be null.';
    elseif (grp is null and rank is not null) then
      return 'Rank must only be set if group name is also provided.';
    elseif (grp is not null and rank is null) then
      return 'Rank must provided if you are creating a grouped label.';
    end if;

    let outcome text := 'Duplicate label name found. Please use a distinct name.';

    outcome := (CALL INTERNAL.VALIDATE_LABEL_CONDITION(:name, :condition));

    if (outcome is not null) then
      return outcome;
    end if;

    BEGIN TRANSACTION;

    -- Make sure that the old name exists once and the new name doesn't exist (assuming it is different from the old name)
    let oldcnt number := (SELECT COUNT(*) AS cnt FROM internal.labels WHERE name = :oldname);
    let newcnt number := (SELECT COUNT(*) AS cnt FROM internal.labels WHERE name = :name AND name <> :oldname);

    IF (oldcnt <> 1) THEN
      outcome := 'Label not found. Please refresh your page to see latest list of labels.';
    ELSEIF (newcnt <> 0) THEN
      outcome := 'A label with this name already exists. Please choose a distinct name.';
    ELSE
      UPDATE internal.labels SET  NAME = :name, GROUP_NAME = :grp, GROUP_RANK = :rank, CONDITION = :condition WHERE NAME = :oldname;
      outcome := null;
    END IF;

    COMMIT;
    CALL INTERNAL.UPDATE_LABEL_VIEW();
    return outcome;
EXCEPTION
  WHEN OTHER THEN
      ROLLBACK;
      RAISE;
END;

CREATE VIEW CATALOG.LABELS IF NOT EXISTS AS SELECT * FROM INTERNAL.LABELS;

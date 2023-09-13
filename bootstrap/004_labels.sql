
CREATE TABLE INTERNAL.LABELS if not exists (name string, group_name string null, group_rank number, label_created_at timestamp, condition string, enabled boolean, label_modified_at timestamp, is_dynamic boolean);

CREATE TABLE INTERNAL.PREDEFINED_LABELS if not exists (name string, group_name string null, group_rank number, label_created_at timestamp, condition string, enabled boolean, label_modified_at timestamp, is_dynamic boolean);

CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_LABELS_TABLE()
RETURNS OBJECT
AS
BEGIN
    -- Add modified at column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'LABELS' AND COLUMN_NAME = 'LABEL_MODIFIED_AT')) THEN
        ALTER TABLE INTERNAL.LABELS ADD COLUMN LABEL_MODIFIED_AT TIMESTAMP;
        UPDATE INTERNAL.LABELS SET LABEL_MODIFIED_AT = CURRENT_TIMESTAMP WHERE LABEL_MODIFIED_AT IS NULL;
    END IF;

    -- Add is_dynamic column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'LABELS' AND COLUMN_NAME = 'IS_DYNAMIC')) THEN
        ALTER TABLE INTERNAL.LABELS ADD COLUMN IS_DYNAMIC BOOLEAN;
        UPDATE INTERNAL.LABELS SET IS_DYNAMIC = FALSE WHERE IS_DYNAMIC IS NULL;
    END IF;

    -- Set ENABLED=TRUE for all labels, until we have a use-case where labels can be disabled.
    update internal.labels set enabled = true where enabled is null;

    -- Recreate the view to avoid number of column mis-match. Should be cheap and only run on install/upgrade, so it's OK
    -- if we run this unnecessarily.
    CREATE OR REPLACE VIEW CATALOG.LABELS COPY GRANTS AS SELECT * FROM INTERNAL.LABELS;
EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'Failed to migrate labels table. ' || :SQLCODE || ': ' || :SQLERRM);
        raise;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_PREDEFINED_LABELS_TABLE()
RETURNS OBJECT
AS
BEGIN
    -- Add modified at column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PREDEFINED_LABELS' AND COLUMN_NAME = 'LABEL_MODIFIED_AT')) THEN
        ALTER TABLE INTERNAL.PREDEFINED_LABELS ADD COLUMN LABEL_MODIFIED_AT TIMESTAMP;
        UPDATE INTERNAL.PREDEFINED_LABELS SET LABEL_MODIFIED_AT = CURRENT_TIMESTAMP WHERE LABEL_MODIFIED_AT IS NULL;
    END IF;

    -- Add is_dynamic column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'PREDEFINED_LABELS' AND COLUMN_NAME = 'IS_DYNAMIC')) THEN
        ALTER TABLE INTERNAL.PREDEFINED_LABELS ADD COLUMN IS_DYNAMIC BOOLEAN;
        UPDATE INTERNAL.PREDEFINED_LABELS SET IS_DYNAMIC = FALSE WHERE IS_DYNAMIC IS NULL;
    END IF;

    -- Set ENABLED=TRUE for all labels, until we have a use-case where labels can be disabled.
    update internal.predefined_labels set enabled = true where enabled is null;

EXCEPTION
    WHEN OTHER THEN
        SYSTEM$LOG('error', 'Failed to migrate PREDEFINED_LABELS table. ' || :SQLCODE || ': ' || :SQLERRM);
        raise;
END;

DROP PROCEDURE IF EXISTS INTERNAL.VALIDATE_LABEL_CONDITION(string, string);

CREATE OR REPLACE PROCEDURE INTERNAL.VALIDATE_LABEL_CONDITION(condition string, is_dynamic boolean)
RETURNS STRING
AS
$$
BEGIN
    let statement string := '';
    if (not is_dynamic) then
        statement := 'select case when \n' || condition || '\n then 1 else 0 end  from reporting.enriched_query_history where false';
    else
        statement := 'select substring(' || condition || ', 0, 0) from reporting.enriched_query_history where false';
    end if;
    execute immediate statement;
    return null;
EXCEPTION
    when statement_error then
        return 'Invalid condition SQL. Please check your syntax.' || :SQLERRM;
    WHEN OTHER THEN
        return 'Failure validating condition. Please check your syntax.' || :SQLERRM;
END;
$$;

DROP PROCEDURE IF EXISTS INTERNAL.VALIDATE_LABEL_Name(string);

-- Verify label name as quoted identifier is not same as any column name in view reporting.enriched_query_history.
CREATE OR REPLACE PROCEDURE INTERNAL.VALIDATE_Name(name string, is_label boolean)
RETURNS STRING
AS
BEGIN
    let statement string := 'select  "' || name || '" from reporting.enriched_query_history where false';
    execute immediate statement;
    if (is_label) then
        return 'Label name can not be same as column name in view reporting.enriched_query_history. Please use a different label name.';
    else
        return 'Group name can not be same as column name in view reporting.enriched_query_history. Please use a different group name.';
    end if;

EXCEPTION
    when statement_error then
        return null;
    WHEN OTHER THEN
        return 'Failure validating name. Please check your syntax.' || :SQLERRM;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.UPDATE_LABEL_VIEW()
RETURNS boolean
AS
BEGIN
    let labels cursor for select name, condition from internal.labels where group_name is null;
    let s string := $$
CREATE OR REPLACE VIEW REPORTING.LABELED_QUERY_HISTORY
COPY GRANTS
AS
SELECT *,$$;
    for label in labels do
         s := s || '\n\tcase when ' || label.condition || ' then true else false end as "' || label.name || '",';
    end for;

    let grouped_labels cursor for
        select group_name, name, condition from internal.labels
        where group_name is not null and NOT is_dynamic
        order by group_name, group_rank;
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

    let dynamic_groups cursor for
        select group_name, condition from internal.labels
        where is_dynamic;
    for dgrp in dynamic_groups do
        s := s || '\n\tiff( ' || dgrp.condition || ' is not null, ' || dgrp.condition || ', \'Other\')  as "' || dgrp.group_name || '",';
    end for;

    s := s || '\n\t1 as not_used_internal\nFROM REPORTING.ENRICHED_QUERY_HISTORY';
    SYSTEM$LOG_INFO('Updating label definitions. Updated SQL: \n' || s);
    --return s;
    execute immediate s;
    return true;
END;

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_LABEL(name text, grp text, rank number, condition text, is_dynamic boolean)
    RETURNS text
    language python
    runtime_version = "3.1-"
    handler = 'create_label'
    packages = ('snowflake-snowpark-python')
    imports=('{{stage}}/crud/crud.zip', '{{stage}}/crud/pydantic.zip')
    EXECUTE AS OWNER
AS
$$
from crud import create_entity
def create_label(session, name, grp, rank, condition, is_dynamic):
    return create_entity(session, 'LABEL', {'name': name, 'group_name': grp, 'group_rank': rank, 'condition': condition, 'is_dynamic': is_dynamic})
$$;

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_LABEL(name text, grp text, rank number, condition text)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
BEGIN
    let outcome text := null;
    outcome := (CALL ADMIN.CREATE_LABEL(:name, :grp, :rank, :condition, false));
    return outcome;
END;
$$;

CREATE OR REPLACE PROCEDURE INTERNAL.INITIALIZE_LABELS()
    RETURNS boolean
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    let labelcnt number := (SELECT COUNT(*) FROM internal.labels);

    let label_inited text := '';
    label_inited := (CALL INTERNAL.get_config('LABELS_INITED'));

    if (labelcnt > 0 OR label_inited = 'True') THEN
        SYSTEM$LOG_INFO('Predefined labels import is skipped. \n');
        RETURN FALSE;
    ELSE
        INSERT INTO INTERNAL.LABELS (NAME, GROUP_NAME, GROUP_RANK, LABEL_CREATED_AT, CONDITION, ENABLED, LABEL_MODIFIED_AT, IS_DYNAMIC)
            SELECT NAME, GROUP_NAME, GROUP_RANK, LABEL_CREATED_AT, CONDITION, ENABLED, LABEL_CREATED_AT, IS_DYNAMIC
            FROM INTERNAL.PREDEFINED_LABELS;
        CALL INTERNAL.SET_CONFIG('LABELS_INITED', 'True');
        SYSTEM$LOG_INFO('Predefined labels are imported into LABELS table. \n');
        RETURN TRUE;
    END IF;
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

CREATE OR REPLACE PROCEDURE ADMIN.DELETE_DYNAMIC_LABEL(name text)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    if (:name is null) then
      return 'Name must not be null.';
    end if;

    DELETE FROM internal.labels where group_name = :name and is_dynamic;
    CALL INTERNAL.UPDATE_LABEL_VIEW();
    return 'done';
END;

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_LABEL(oldname text, name text, grp text, rank number, condition text, is_dynamic boolean)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    if (:is_dynamic = true) then
        if (:grp is null) then
            return 'group name must be set for dynamic grouped labels.';
        elseif (:oldname is not null or :name is not null or :rank is not null) then
            return 'Rank or name must not be set for dynamic grouped labels.';
        end if;
    else
        if (name is null) then
          return 'Name must not be null.';
        elseif (grp is null and rank is not null) then
          return 'Rank must only be set if group name is also provided.';
        elseif (grp is not null and rank is null) then
          return 'Rank must provided if you are creating a grouped label.';
        end if;
    end if;

    let outcome text := 'Duplicate label name found. Please use a distinct name.';

    outcome := (CALL INTERNAL.VALIDATE_LABEL_CONDITION(:condition, :is_dynamic));
    if (outcome is not null) then
      return outcome;
    end if;

    outcome := (CALL INTERNAL.VALIDATE_Name(:name, true));

    if (outcome is not null) then
      return outcome;
    end if;

    BEGIN TRANSACTION;

    if (:is_dynamic = false) then
        -- Make sure that the old name exists once and the new name doesn't exist (assuming it is different from the old name)
        let oldcnt number := (SELECT COUNT(*) AS cnt FROM internal.labels WHERE name = :oldname);
        let newcnt number := (SELECT COUNT(*) AS cnt FROM internal.labels WHERE name = :name AND name <> :oldname);

        IF (oldcnt <> 1) THEN
          outcome := 'Label not found. Please refresh your page to see latest list of labels.';
        ELSEIF (newcnt <> 0) THEN
          outcome := 'A label with this name already exists. Please choose a distinct name.';
        ELSE
          UPDATE internal.labels SET  NAME = :name, GROUP_NAME = :grp, GROUP_RANK = :rank, CONDITION = :condition, LABEL_MODIFIED_AT = current_timestamp() WHERE NAME = :oldname;
          outcome := null;
        END IF;
    else
        -- dynamic grouped label
        let oldcnt number := (SELECT COUNT(*) AS cnt FROM internal.labels WHERE group_name = :grp and is_dynamic);
        IF (oldcnt <> 1) THEN
          outcome := 'Label not found. Please refresh your page to see latest list of labels.';
        ELSE
          UPDATE internal.labels SET  CONDITION = :condition, LABEL_MODIFIED_AT = current_timestamp() WHERE group_name = :grp and is_dynamic;
          outcome := null;
        END IF;
    end if;

    COMMIT;
    CALL INTERNAL.UPDATE_LABEL_VIEW();
    return outcome;
EXCEPTION
  WHEN OTHER THEN
      ROLLBACK;
      RAISE;
END;

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_LABEL(oldname text, name text, grp text, rank number, condition text)
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    let outcome text := null;
    outcome := (CALL ADMIN.UPDATE_LABEL(:oldname, :name, :grp, :rank, :condition, false));
    return outcome;
END;

CREATE OR REPLACE VIEW CATALOG.LABELS AS SELECT * exclude (enabled) FROM INTERNAL.LABELS;

CREATE OR REPLACE PROCEDURE INTERNAL.POPULATE_PREDEFINED_LABELS()
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    let query_hash_enabled boolean := (select system$BEHAVIOR_CHANGE_BUNDLE_STATUS('2023_06') = 'ENABLED');
    MERGE INTO internal.predefined_labels t
    USING (
        SELECT *
        from (values
                ('Large Results', 'rows_produced > 50000000', TRUE),
                ('Writes', 'query_type in (\'CREATE_TABLE_AS_SELECT\', \'INSERT\')', TRUE),
                ('Expanding Output', '10*bytes_scanned < BYTES_WRITTEN_TO_RESULT', TRUE),
                ('Full Scans', 'coalesce(partitions_scanned, 0) > coalesce(partitions_total, 1) * 0.95', TRUE),
                ('Long Compilation', 'COMPILATION_TIME > 100', TRUE),
                ('Long Queries', 'TOTAL_ELAPSED_TIME > 600000', TRUE),
                ('Expensive Queries', 'COST>0.5', TRUE),
                ('Accelerated Queries', 'QUERY_ACCELERATION_BYTES_SCANNED > 0', TRUE),
                ('Reoccurring Queries', 'tools.is_reoccurring_query(query_parameterized_hash, 1000)', :query_hash_enabled),
                ('ad-hoc Queries', 'tools.is_ad_hoc_query(query_parameterized_hash, 10)', :query_hash_enabled),
                ('dbt Queries', 'array_contains(\'dbt\'::variant, tools.qtag_sources(qtag_filter))', TRUE)
             ) where $3) s (name, condition, enabled)
    ON t.name = s.name
    WHEN MATCHED THEN
    UPDATE
        SET t.GROUP_NAME = NULL, t.GROUP_RANK = NULL, t.CONDITION = s.condition, t.LABEL_MODIFIED_AT = current_timestamp(),
            T.IS_DYNAMIC = FALSE, T.ENABLED = TRUE
    WHEN NOT MATCHED THEN
    INSERT
        ("NAME", "GROUP_NAME", "GROUP_RANK", "LABEL_CREATED_AT", "CONDITION", "LABEL_MODIFIED_AT", "IS_DYNAMIC", "ENABLED")
        VALUES (s.name, NULL, NULL,  current_timestamp(), s.condition, current_timestamp(), FALSE, TRUE);

    -- populate for dynamic labels
    MERGE INTO internal.predefined_labels t
    USING (
        SELECT *
        from (values
            ('dbt Models', 'tools.qtag_value(qtag_filter, \'dbt\', \'node_id\')'),
            ('qtag Sources', 'tools.qtag_sources(qtag_filter)[0]')
             )) s (group_name, condition)
    ON t.group_name = s.group_name
    WHEN MATCHED THEN
    UPDATE
        SET t.GROUP_NAME = s.group_name, t.GROUP_RANK = NULL, t.CONDITION = s.condition, t.LABEL_MODIFIED_AT = current_timestamp(),
            T.IS_DYNAMIC = TRUE, T.ENABLED = TRUE
    WHEN NOT MATCHED THEN
    INSERT
        ("NAME", "GROUP_NAME", "GROUP_RANK", "LABEL_CREATED_AT", "CONDITION", "LABEL_MODIFIED_AT", "IS_DYNAMIC", "ENABLED")
        VALUES (NULL, s.group_name, NULL,  current_timestamp(), s.condition, current_timestamp(), TRUE, TRUE);

    RETURN NULL;
EXCEPTION
  WHEN OTHER THEN
      ROLLBACK;
      RAISE;
END;

CREATE OR REPLACE PROCEDURE INTERNAL.VALIDATE_PREDEFINED_LABELS()
    RETURNS text
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
DECLARE
    outcome string;
    labels cursor for select "NAME", "CONDITION" from internal.predefined_labels;
BEGIN
    for record in labels do
        let name string := record."NAME";
        let condition string := record."CONDITION";
        outcome := (CALL INTERNAL.VALIDATE_LABEL_CONDITION(:condition, false));
        IF (outcome is not null) then
            let res text := 'Predefined label  \'' || name || '\' with condition  \'' || condition || '\' is not valid';
            RETURN res;
        END IF;
    end for;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE PROCEDURE INTERNAL.MERGE_PREDEFINED_LABELS()
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
    insert into internal.labels ("NAME", "GROUP_NAME", "GROUP_RANK", "LABEL_CREATED_AT", "CONDITION", "LABEL_MODIFIED_AT", "IS_DYNAMIC") select name, group_name, group_rank, label_created_at, condition, label_modified_at, IS_DYNAMIC from internal.predefined_labels where name not in (select name from internal.labels);
$$;

CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_PREDEFINED_LABELS(gap_in_seconds NUMBER)
    RETURNS BOOLEAN
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
BEGIN
    let rowCount1 number := (
        WITH
        OLD_PREDEFINED_LABELS AS
            (SELECT name, LABEL_CREATED_AT FROM INTERNAL.PREDEFINED_LABELS WHERE TIMESTAMPDIFF(SECOND, LABEL_CREATED_AT, CURRENT_TIMESTAMP) > :gap_in_seconds),
        USER_LABELS AS
            (SELECT name, LABEL_MODIFIED_AT FROM INTERNAL.LABELS)
        SELECT count(*) from (select * from OLD_PREDEFINED_LABELS MINUS SELECT * FROM USER_LABELS) S
        );
    let rowCount2 number := (
        WITH
        OLD_PREDEFINED_LABELS AS
            (SELECT name, LABEL_CREATED_AT FROM INTERNAL.PREDEFINED_LABELS WHERE TIMESTAMPDIFF(SECOND, LABEL_CREATED_AT, CURRENT_TIMESTAMP) > :gap_in_seconds),
        USER_LABELS AS
            (SELECT name, LABEL_MODIFIED_AT FROM INTERNAL.LABELS)
        SELECT count(*) from (select * from USER_LABELS MINUS SELECT * FROM OLD_PREDEFINED_LABELS) S
        );

    IF (rowCount1 > 0 OR rowCount2 > 0) THEN
        RETURN FALSE;
    END IF;

    MERGE INTO internal.labels t
    USING internal.predefined_labels s
    ON t.name = s.name
    WHEN MATCHED THEN
    UPDATE
        SET t.GROUP_NAME = s.GROUP_NAME, t.GROUP_RANK = s.GROUP_RANK, t.CONDITION = s.condition, t.LABEL_MODIFIED_AT = s.LABEL_CREATED_AT, t.IS_DYNAMIC = s.IS_DYNAMIC, t.ENABLED = s.ENABLED
    WHEN NOT MATCHED THEN
    INSERT
        ("NAME", "GROUP_NAME", "GROUP_RANK", "LABEL_CREATED_AT", "CONDITION", "LABEL_MODIFIED_AT", "IS_DYNAMIC", "ENABLED")
        VALUES (s.name, s.GROUP_NAME, s.GROUP_RANK,  S.LABEL_CREATED_AT, s.condition, S.LABEL_CREATED_AT, S.IS_DYNAMIC, S.ENABLED);

    return TRUE;
END;
$$;

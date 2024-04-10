
CREATE TABLE INTERNAL.LABELS if not exists (name string, group_name string null, group_rank number, label_created_at timestamp, condition string, enabled boolean, label_modified_at timestamp, is_dynamic boolean, label_id string);

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

    -- Add unique id column
    IF (NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'INTERNAL' AND TABLE_NAME = 'LABELS' AND COLUMN_NAME = 'LABEL_ID')) THEN
        ALTER TABLE INTERNAL.LABELS ADD COLUMN LABEL_ID STRING;
        UPDATE INTERNAL.LABELS SET LABEL_ID = UUID_STRING() WHERE LABEL_ID IS NULL;
    END IF;


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

CREATE OR REPLACE FUNCTION VALIDATION.IS_NULL(name text)
    RETURNS text
    COMMENT='Validates that a field in an object is null'
AS
$$
    tools.templatejs('TO_CHAR(f:{name}) is null', {'name': name})
$$;

CREATE OR REPLACE FUNCTION VALIDATION.NOT_NULL(name text)
    RETURNS text
    COMMENT='Validates that a field in an object is not null'
AS
$$
    tools.templatejs('TO_CHAR(f:{name}) is not null', {'name': name})
$$;

CREATE OR REPLACE FUNCTION VALIDATION.NO_MATCHING_ROWS(table_name text, uniq_column_name text, null_column_name text)
    RETURNS text
    COMMENT='Validates that there are no rows in the table with the given columns'
AS
$$
    tools.templatejs('(select count(\*) = 0 from {table_name} where {null_column_name} is null and {uniq_column_name} = f:{uniq_column_name})',
        {'table_name': table_name, 'uniq_column_name': uniq_column_name, 'null_column_name': null_column_name})
$$;

-- Ungrouped labels
CREATE OR REPLACE VIEW INTERNAL.VALIDATE_LABELS AS
    select $1 as sql, $2 as message, $3 as simple, $4 as create_only from (
        -- Basic null checks, converting variant null to sql null
        select VALIDATION.NOT_NULL('name'), 'Name must not be null', true, false
        UNION ALL
        select VALIDATION.NOT_NULL('condition'), 'Condition must not be null', true, false
        UNION ALL
        -- group_name and group_rank must be null
        select VALIDATION.IS_NULL('group_name') || ' and ' || VALIDATION.IS_NULL('group_rank'), 'Group rank may only be provided for grouped labels', true, false
        UNION ALL
        -- label name must be unique among all names
        SELECT VALIDATION.NO_MATCHING_ROWS('internal.labels', 'name', 'group_name'), 'A label with this name already exists', true, true
        UNION ALL
        -- label name must be unique across all group_names
        SELECT '(select count(*) = 0 from internal.labels where group_name = f:name and group_name is not null)', 'A label group already exists with this name', true, true
        UNION ALL
        -- Condition must compile
        SELECT 'with result as procedure (input varchar) returns boolean language sql as \$\$ begin let c varchar := (select parse_json(:input):condition);execute immediate \'select case when \' || :c || \' then true else false end from reporting.enriched_query_history limit 1\';return true;end;\$\$ call result(?);', 'Invalid label condition', false, false
        UNION ALL
        -- make sure label name doesn't exist as query history column
        SELECT 'with result as procedure (input varchar) returns boolean language sql as \$\$ begin let n varchar := (select parse_json(:input):name); execute immediate \'select \' || :n || \' from reporting.enriched_query_history where false\'; return false; exception when statement_error then return true; when other then return false; end;\$\$ call result(?);', 'Label name cannot be the same as a column in REPORTING.ENRICHED_QUERY_HISTORY', false, false
    );

-- Grouped labels
CREATE OR REPLACE VIEW INTERNAL.VALIDATE_GROUPED_LABELS AS
    select $1 as sql, $2 as message, $3 as simple, $4 as create_only from (
        -- Basic null checks, converting variant null to sql null
        SELECT VALIDATION.NOT_NULL('group_name'), 'Group name must not be null', true, false
        UNION ALL
        SELECT VALIDATION.NOT_NULL('name'), 'Name must not be null', true, false
        UNION ALL
        SELECT VALIDATION.NOT_NULL('condition'), 'Condition must not be null', true, false
        UNION ALL
        SELECT VALIDATION.NOT_NULL('group_rank'), 'Grouped labels must have a rank', true, false
        UNION ALL
        -- label name must be unique in this group
        SELECT '(select count(*) = 0 from internal.labels where group_name = f:group_name and name = f:name)', 'A label with this name already exists', true, true
        UNION ALL
        -- label group name must be unique across all label names
        SELECT '(select count(*) = 0 from internal.labels where name = f:group_name and group_name is null)', 'A label with this name already exists', true, true
        UNION ALL
        -- label name must be unique across dynamic grouped labels
        SELECT VALIDATION.NO_MATCHING_ROWS('internal.labels', 'group_name', 'name'), 'A label with this name already exists', true, true
        UNION ALL
        -- group rank must be unique across labels in a group
        SELECT '(select count(*) = 0 from internal.labels where group_name = f:group_name and group_rank = f:group_rank)', 'A label already already exists with this rank', true, true
        UNION ALL
        -- Condition must compile
        SELECT 'with result as procedure (input varchar) returns boolean language sql as \$\$ begin let c varchar := (select parse_json(:input):condition);execute immediate \'select case when \' || :c || \' then true else false end from reporting.enriched_query_history limit 1\';return true;end;\$\$ call result(?);', 'Invalid label condition', false, false
        UNION ALL
        -- make sure label group name doesn't exist as query history column
        SELECT 'with result as procedure (input varchar) returns boolean language sql as \$\$ begin let n varchar := (select parse_json(:input):group_name); execute immediate \'select \' || :n || \' from reporting.enriched_query_history where false\'; return false; exception when statement_error then return true; when other then return false; end;\$\$ call result(?);', 'Label group name cannot be the same as a column in REPORTING.ENRICHED_QUERY_HISTORY', false, false
    );

-- Dynamic grouped labels
CREATE OR REPLACE VIEW INTERNAL.VALIDATE_DYNAMIC_LABELS AS
    select $1 as sql, $2 as message, $3 as simple, $4 as create_only from (
        -- Basic null checks, converting variant null to sql null
        SELECT VALIDATION.NOT_NULL('group_name'), 'Dynamic labels must have a group name', true, false
        UNION ALL
        SELECT VALIDATION.NOT_NULL('condition'), 'Dynamic labels must have a condition', true, false
        UNION ALL
        SELECT VALIDATION.IS_NULL('name'), 'Dynamic labels cannot have a name', true, false
        UNION ALL
        SELECT VALIDATION.IS_NULL('group_rank'), 'Dynamic labels cannot have a rank', true, false
        UNION ALL
        -- label name must be unique in this group
        SELECT '(select count(*) = 0 from internal.labels where group_name = f:group_name and name = f:name)', 'A label already exists with this name', true, true
        UNION ALL
        -- label group name must be unique across all label names
        SELECT '(select count(*) = 0 from internal.labels where name = f:group_name and group_name is null)', 'A label already exists with this name', true, true
        UNION ALL
        -- label name must be unique across dynamic grouped labels
        SELECT '(select count(*) = 0 from internal.labels where group_name = f:group_name and name is null)', 'A label already exists with this name', true, true
        UNION ALL
        -- group rank must be unique across labels in a group
        SELECT '(select count(*) = 0 from internal.labels where group_name = f:group_name and group_rank = f:group_rank)', 'A label already already exists with this rank', true, true
        UNION ALL
        -- Condition must compile
        SELECT 'with result as procedure (input varchar) returns boolean language sql as \$\$ begin let c varchar := (select parse_json(:input):condition);execute immediate \'select substring(\' || :c || \', 0, 0)  from reporting.enriched_query_history where false\';return true;end;\$\$ call result(?);', 'Invalid label condition', false, false
        UNION ALL
        -- make sure label group name doesn't exist as query history column
        SELECT 'with result as procedure (input varchar) returns boolean language sql as \$\$ begin let n varchar := (select parse_json(:input):group_name); execute immediate \'select \' || :n || \' from reporting.enriched_query_history where false\'; return false; exception when statement_error then return true; when other then return false; end;\$\$ call result(?);', 'Label group name cannot duplicate a column in REPORTING.ENRICHED_QUERY_HISTORY', false, false
    );


CREATE OR REPLACE PROCEDURE ADMIN.CREATE_LABEL(name text, grp text, rank number, condition text, is_dynamic boolean)
    returns text
    language sql
    EXECUTE AS OWNER
AS
$$
BEGIN
    let label object := (select object_construct_keep_null('name', :name, 'group_name', :grp, 'group_rank', :rank, 'condition', :condition, 'is_dynamic', :is_dynamic, 'enabled', TRUE, 'label_modified_at', current_timestamp(), 'label_id', uuid_string(), 'label_created_at', current_timestamp()));
    let validation_err text;
    let kind text := 'labels';
    if (is_dynamic) then
        kind := 'dynamic_labels';
    elseif (grp is not null) then
        kind := 'grouped_labels';
    end if;
    call admin.validate(:label, :kind, TRUE) into :validation_err;
    if (:validation_err is not null) then
        return :validation_err;
    end if;
    call admin.write(:label, 'labels', 'label_id');
END;
$$;

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_LABEL(name text, grp text, rank number, condition text)
    returns text
    language sql
    EXECUTE AS OWNER
AS
$$
BEGIN
    let label object := (select object_construct_keep_null('name', :name, 'group_name', :grp, 'group_rank', :rank, 'condition', :condition, 'is_dynamic', FALSE, 'enabled', TRUE, 'label_modified_at', current_timestamp(), 'label_id', uuid_string(), 'label_created_at', current_timestamp()));
    let validation_err text;
    let kind text := 'labels';
    if (grp is not null) then
        kind := 'grouped_labels';
    end if;
    call admin.validate(:label, :kind, TRUE) into :validation_err;
    if (:validation_err is not null) then
        return :validation_err;
    end if;
    call admin.write(:label, 'labels', 'label_id');
    call INTERNAL.UPDATE_LABEL_VIEW();
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
        INSERT INTO INTERNAL.LABELS (NAME, GROUP_NAME, GROUP_RANK, LABEL_CREATED_AT, CONDITION, ENABLED, LABEL_MODIFIED_AT, IS_DYNAMIC, LABEL_ID)
            SELECT NAME, GROUP_NAME, GROUP_RANK, LABEL_CREATED_AT, CONDITION, ENABLED, LABEL_CREATED_AT, IS_DYNAMIC, UUID_STRING()
            FROM INTERNAL.PREDEFINED_LABELS;
        CALL INTERNAL.SET_CONFIG('LABELS_INITED', 'True');
        SYSTEM$LOG_INFO('Predefined labels are imported into LABELS table. \n');
        RETURN TRUE;
    END IF;
END;


CREATE OR REPLACE PROCEDURE ADMIN.DELETE_LABEL(name text)
    RETURNS text
    language sql
    EXECUTE AS OWNER
AS
$$
BEGIN
    if (name is null) then
        return 'Name must not be null';
    end if;
    delete from internal.labels where name = :name;
    call INTERNAL.UPDATE_LABEL_VIEW();
END;
$$;

-- TODO Differentiate from ungrouped labels  by having "AND IS_DYNAMIC" in the delete clause
CREATE OR REPLACE PROCEDURE ADMIN.DELETE_DYNAMIC_LABEL(name text)
    RETURNS text
    language sql
    EXECUTE AS OWNER
AS
$$
BEGIN
    if (name is null) then
        return 'Name must not be null';
    end if;
    delete from internal.labels where name = :name;
    call INTERNAL.UPDATE_LABEL_VIEW();
END;
$$;


CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_LABEL(oldname text, name text, grp text, rank number, condition text, is_dynamic boolean)
    RETURNS text
    language sql
    EXECUTE AS OWNER
 AS
$$
BEGIN
    let uuid string;
    if (is_dynamic) then
        -- dynamic labels use group_name
        uuid := (select label_id from internal.labels where group_name = :oldname and is_dynamic);
    else
        uuid := (select label_id from internal.labels where name = :oldname and not is_dynamic);
    end if;
    if (uuid is null) then
        return 'A label with the name ' || oldname || ' does not exist';
    end if;

    -- If the label name changed on update, we want to run the unique name checks like we do on create.
    let name_changed boolean;
    if (is_dynamic) then
        name_changed := (select :oldname <> :grp);
    else
        name_changed := (select :oldname <> :name);
    end if;

    let created_at timestamp;
    if (is_dynamic) then
        created_at := (select label_created_at from internal.labels where group_name = :oldname);
    else
        created_at := (select label_created_at from internal.labels where name = :oldname);
    end if;

    let label object := (select object_construct_keep_null('name', :name, 'group_name', :grp, 'group_rank', :rank, 'condition', :condition, 'is_dynamic', :is_dynamic, 'enabled', TRUE, 'label_modified_at', current_timestamp(), 'label_id', :uuid, 'label_created_at', :created_at));
    let validation_err text;
    let kind text := 'labels';
    if (is_dynamic) then
        kind := 'dynamic_labels';
    end if;
    call admin.validate(:label, :kind, :name_changed) into :validation_err;
    if (:validation_err is not null) then
        return :validation_err;
    end if;
    call admin.write(:label, 'labels', 'label_id');
    call INTERNAL.UPDATE_LABEL_VIEW();
END;
$$;

CREATE OR REPLACE PROCEDURE ADMIN.UPDATE_LABEL(oldname text, name text, grp text, rank number, condition text)
    RETURNS text
    language sql
    EXECUTE AS OWNER
 AS
$$
BEGIN
    let uuid string := (select label_id from internal.labels where name = :oldname and is_dynamic <> TRUE);
    if (uuid is null) then
        return 'A label with the name ' || oldname || ' does not exist';
    end if;

    -- If the label name changed on update, we want to run the unique name checks like we do on create.
    let name_changed boolean := (select :oldname <> :name);
    let created_at timestamp := (select label_created_at from internal.labels where name = :oldname);
    let label object := (select object_construct_keep_null('name', :name, 'group_name', :grp, 'group_rank', :rank, 'condition', :condition, 'is_dynamic', FALSE, 'enabled', TRUE, 'label_modified_at', current_timestamp(), 'label_id', :uuid, 'label_created_at', :created_at));
    let kind text := 'labels';
    if (grp is not null) then
        kind := 'grouped_labels';
    end if;
    let validation_err text;
    call admin.validate(:label, :kind, :name_changed) into :validation_err;
    if (:validation_err is not null) then
        return :validation_err;
    end if;
    call admin.write(:label, 'labels', 'label_id');
    call INTERNAL.UPDATE_LABEL_VIEW();
END;
$$;

CREATE OR REPLACE VIEW CATALOG.LABELS AS SELECT * FROM INTERNAL.LABELS;

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

CREATE OR REPLACE PROCEDURE INTERNAL.MERGE_PREDEFINED_LABELS()
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
    insert into internal.labels ("NAME", "GROUP_NAME", "GROUP_RANK", "LABEL_CREATED_AT", "CONDITION", "LABEL_MODIFIED_AT", "IS_DYNAMIC", "LABEL_ID", "ENABLED") select name, group_name, group_rank, label_created_at, condition, label_modified_at, IS_DYNAMIC, UUID_STRING(), ENABLED from internal.predefined_labels where name not in (select name from internal.labels);
$$;


CREATE OR REPLACE PROCEDURE INTERNAL.MIGRATE_PREDEFINED_LABELS(gap_in_seconds NUMBER)
    RETURNS BOOLEAN
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
BEGIN
    -- Count the number of predefined labels that have newer creation time than user-defined labels' modification time.
    let rowCount1 number := (
        WITH
        OLD_PREDEFINED_LABELS AS
            (SELECT name, group_name, LABEL_CREATED_AT FROM INTERNAL.PREDEFINED_LABELS WHERE TIMESTAMPDIFF(SECOND, LABEL_CREATED_AT, CURRENT_TIMESTAMP) > :gap_in_seconds),
        USER_LABELS AS
            (SELECT name, group_name, LABEL_MODIFIED_AT FROM INTERNAL.LABELS)
        SELECT count(*) from (select * from OLD_PREDEFINED_LABELS MINUS SELECT * FROM USER_LABELS) S
        );
    -- Count the number of user-defined labels that have newer modification time than predefined labels' creation time.
    let rowCount2 number := (
        WITH
        OLD_PREDEFINED_LABELS AS
            (SELECT name, group_name, LABEL_CREATED_AT FROM INTERNAL.PREDEFINED_LABELS WHERE TIMESTAMPDIFF(SECOND, LABEL_CREATED_AT, CURRENT_TIMESTAMP) > :gap_in_seconds),
        USER_LABELS AS
            (SELECT name, group_name, LABEL_MODIFIED_AT FROM INTERNAL.LABELS)
        SELECT count(*) from (select * from USER_LABELS MINUS SELECT * FROM OLD_PREDEFINED_LABELS) S
        );

    -- If there is any difference, we treat this as the user having changed the labels, and we don't reset these labels.
    IF (rowCount1 > 0 OR rowCount2 > 0) THEN
        RETURN FALSE;
    END IF;

    -- ungrouped labels
    MERGE INTO internal.labels t
    USING (
        select * from internal.predefined_labels where name is not null
    ) s ON t.name = s.name
    WHEN MATCHED THEN
    UPDATE
        SET t.GROUP_NAME = s.GROUP_NAME, t.GROUP_RANK = s.GROUP_RANK, t.CONDITION = s.condition, t.LABEL_MODIFIED_AT = s.LABEL_CREATED_AT, t.IS_DYNAMIC = s.IS_DYNAMIC, t.ENABLED = s.ENABLED
    WHEN NOT MATCHED THEN
    INSERT
        ("NAME", "GROUP_NAME", "GROUP_RANK", "LABEL_CREATED_AT", "CONDITION", "LABEL_MODIFIED_AT", "IS_DYNAMIC", "ENABLED", "LABEL_ID")
        VALUES (s.name, s.GROUP_NAME, s.GROUP_RANK,  S.LABEL_CREATED_AT, s.condition, S.LABEL_CREATED_AT, S.IS_DYNAMIC, S.ENABLED, UUID_STRING());

    -- grouped and dynamic grouped labels
    MERGE INTO internal.labels t
    USING (
        select * from internal.predefined_labels where name is null
    ) s ON t.group_name = s.group_name
    WHEN MATCHED THEN
    UPDATE
        SET t.GROUP_NAME = s.GROUP_NAME, t.GROUP_RANK = s.GROUP_RANK, t.CONDITION = s.condition, t.LABEL_MODIFIED_AT = s.LABEL_CREATED_AT, t.IS_DYNAMIC = s.IS_DYNAMIC, t.ENABLED = s.ENABLED
    WHEN NOT MATCHED THEN
    INSERT
        ("NAME", "GROUP_NAME", "GROUP_RANK", "LABEL_CREATED_AT", "CONDITION", "LABEL_MODIFIED_AT", "IS_DYNAMIC", "ENABLED", "LABEL_ID")
        VALUES (s.name, s.GROUP_NAME, s.GROUP_RANK,  S.LABEL_CREATED_AT, s.condition, S.LABEL_CREATED_AT, S.IS_DYNAMIC, S.ENABLED, UUID_STRING());

    return TRUE;
END;
$$;


CREATE OR REPLACE PROCEDURE INTERNAL.REMOVE_DUPLICATE_LABELS()
    RETURNS STRING
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
$$
BEGIN
        -- Migrate ungrouped labels
        delete from internal.labels t using(
          select name, label_id, row_number() over (partition by name order by label_id) as rn from internal.labels where group_name is null
          ) s where s.name = t.name and t.group_name is null and s.label_id != t.label_id and s.rn > 1;

        -- Migrate grouped labels
        delete from internal.labels t using(
          select name, group_name, label_id, row_number() over (partition by name, group_name order by label_id) as rn from internal.labels where group_name is not null
          ) s where (s.name = t.name or t.name is null) and t.group_name =s.group_name and s.label_id != t.label_id and s.rn > 1;

END;
$$;

-- Delete any duplicate labels from a previous bugfix. Run this ASAP to prevent any future code from failing because
-- we happen to have latent duplicated labels.
-- call INTERNAL.REMOVE_DUPLICATE_LABELS();

-- Remove any outdated objects
DROP PROCEDURE IF EXISTS INTERNAL.VALIDATE_LABEL_CONDITION(string, string);
DROP PROCEDURE IF EXISTS INTERNAL.VALIDATE_LABEL_CONDITION(string, boolean);
DROP PROCEDURE IF EXISTS INTERNAL.VALIDATE_LABEL_Name(string);
DROP PROCEDURE IF EXISTS INTERNAL.VALIDATE_Name(string, boolean);

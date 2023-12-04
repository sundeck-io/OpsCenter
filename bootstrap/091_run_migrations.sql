
-- Run all migration procedures for INTERNAL tables (not a versioned schema).
-- Only run procedures that do not rely on access to the SNOWFLAKE database because we may not have access to it yet.

-- Migrate the schema of probes and labels tables
call INTERNAL.MIGRATE_PROBES_TABLE();
call INTERNAL.MIGRATE_LABELS_TABLE();
call INTERNAL.MIGRATE_PREDEFINED_PROBES_TABLE();
call INTERNAL.MIGRATE_PREDEFINED_LABELS_TABLE();

-- create the query_hash functions in TOOLS
call INTERNAL.ENABLE_QUERY_HASH();

-- Migrate warehouse schedules table
call INTERNAL.MIGRATE_WHSCHED_TABLE();

-- Populate the list of predefined labels
call INTERNAL.POPULATE_PREDEFINED_LABELS();

-- Init labels using predefined_labels, if the consumer account has not call INTERNAL.INITIALIZE_LABELS, and it
-- does not have user-created labels.
call INTERNAL.INITIALIZE_LABELS();

-- Migrate predefined labels to user's labels if user
-- 1) does not make any change to predefined label,
-- 2) and does not create new user label
-- after last install/upgrade of APP
-- parameter 7200 (seconds) is the timestamp difference when a predefined label is regarded as an old one.
call INTERNAL.MIGRATE_PREDEFINED_LABELS(7200);

-- Populate the list of predefined probes
call INTERNAL.POPULATE_PREDEFINED_PROBES();

-- Init PROBES using predefined_probes, if the consumer account has not call INTERNAL.INITIALIZE_PROBES, and it
-- does not have user-created probes.
call INTERNAL.INITIALIZE_PROBES();

-- Migrate predefined probes to user's probes if user
-- 1) does not make any change to predefined probes,
-- 2) and does not create new user probe
-- after last install/upgrade of APP
-- parameter 7200 (seconds) is the timestamp difference when a predefined probe is regarded as an old one.
call INTERNAL.MIGRATE_PREDEFINED_PROBES(7200);

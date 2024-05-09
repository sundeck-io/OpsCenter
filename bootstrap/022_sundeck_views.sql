

create or replace view CATALOG.BROKERS as
 with b as (
 select p.value FROM TABLE(FLATTEN(input => parse_json(internal.wrapper_ef_run(object_construct('SQLText', 'SHOW SUNDECK FLOWS'))))) p
)

select
    hex_decode_string(b.VALUE:NAME) as name,
    hex_decode_string(b.VALUE:IS_DEFAULT) as is_default,
    hex_decode_string(b.VALUE:DEFAULT_WAREHOUSE) as default_warehouse,
    hex_decode_string(b.VALUE:HOSTNAME) as hostname,
    hex_decode_string(b.VALUE:LAST_MODIFIED_AT) as last_modified_at,
    hex_decode_string(b.VALUE:DDL) as ddl,
    hex_decode_string(b.VALUE:RESULTS_PATH) as results_path,
    hex_decode_string(b.VALUE:COMMENT) as comment
from b;


create or replace view CATALOG.ROUTINES as
 with b as (
 select p.value FROM TABLE(FLATTEN(input => parse_json(internal.wrapper_ef_run(object_construct('SQLText', 'SHOW SUNDECK ROUTINES'))))) p
)

select
    hex_decode_string(b.VALUE:ROUTINE_NAME) as name,
    hex_decode_string(b.VALUE:ROUTINE_NAMESPACE) as namespace,
    hex_decode_string(b.VALUE:ROUTINE_REF_COUNT) as ref_count,
    hex_decode_string(b.VALUE:ROUTINE_CREATED_AT) as created_at,
    hex_decode_string(b.VALUE:ROUTINE_LAST_MODIFIED_AT) as last_modified_at,
    hex_decode_string(b.VALUE:ROUTINE_DDL) as ddl,
    hex_decode_string(b.VALUE:ROUTINE_COMMENT) as comment
from b;

create or replace view CATALOG.PINS as
 with b as (
 select p.value FROM TABLE(FLATTEN(input => parse_json(internal.wrapper_ef_run(object_construct('SQLText', 'SHOW SUNDECK TABLES'))))) p
)

select
    hex_decode_string(b.VALUE:PIN_DB_NAME) as db_name,
    hex_decode_string(b.VALUE:PIN_SCHEMA_NAME) as schema_name,
    hex_decode_string(b.VALUE:PIN_TABLE_NAME) as table_name,
    hex_decode_string(b.VALUE:PIN_CREATED_AT) as created_at,
    hex_decode_string(b.VALUE:PIN_IS_SYSTEM) as is_system,
    hex_decode_string(b.VALUE:PIN_REFRESH_FREQUENCY_SECONDS) as refresh_frequency_seconds,
from b;


create or replace function tools.tables(sql varchar, database varchar, schema varchar)
returns array
language python
runtime_version=3.8
imports=('{{stage}}/python/sqlglot.zip')
handler='test'
as
$$
from sqlglot import parse_one
import sqlglot.expressions as exp
def test(sql, database, schema):
    try:
        return list(f"{database if table.catalog == '' else table.catalog}.{schema if table.db == '' else table.db}.{table.name}".upper() for
        table in parse_one(sql, read='snowflake').find_all(exp.Table))
    except:
        return list()
$$;

create or replace function tools.tables(sql varchar)
returns array
as
$$
tools.tables(sql, current_database(), current_schema())
$$;

create or replace function tools.tables_contains(sql varchar, tbl varchar)
returns boolean
as
$$
array_contains(upper(tbl)::variant, tools.tables(sql))
$$;

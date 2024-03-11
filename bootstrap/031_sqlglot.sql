
drop function if exists tools.tables(varchar);
drop function if exists tools.tables(varchar, varchar);
drop function if exists tools.tables(varchar, varchar, varchar);

create or replace function tools.tables(sql varchar, database varchar default current_database(), schema varchar default current_schema(), exclude_cte boolean default true)
returns array
language python
runtime_version=3.8
imports=('{{stage}}/python/sqlglot.zip')
handler='test'
as
$$
from sqlglot import parse_one
import sqlglot.expressions as exp
def test(sql, database, schema, exclude_cte):
    if schema is None:
        schema = ''
    if database is None:
        database = ''
    try:
        tables = list(f"{database if table.catalog == '' else table.catalog}.{schema if table.db == '' else table.db}.{table.name}".upper() for
        table in parse_one(sql, read='snowflake').find_all(exp.Table))
        if exclude_cte:
            ctes = list(f"{database}.{schema}.{cte.alias}".upper() for cte in parse_one(sql, read='snowflake').find_all(exp.CTE))
            tables = list(set(tables) - set(ctes))
        else:
            tables = list(set(tables))
        return tables
    except:
        return list()
def cte_aliases(sql, database, schema):
    try:
        ctes = list(f"{database}.{schema}.{cte.alias}".upper() for cte in parse_one(sql, read='snowflake').find_all(exp.CTE))
    except:
        return list()
    return ctes
$$;

create or replace function tools.normalize(sql varchar, database varchar, schema varchar, include_comments boolean default true)
returns text
language python
runtime_version=3.8
packages = ('pandas')
imports=('{{stage}}/python/sqlglot.zip')
handler='parse_all'
as
$$
import pandas
from _snowflake import vectorized
from sqlglot import parse_one, optimizer
import sqlglot.expressions as exp
import hashlib
import random

@vectorized(input=pandas.DataFrame)
def parse_all(df):
  return df.apply(lambda row: parse(row[0], row[1], row[2], row[3]), axis=1)

def transform(node, database, schema):
    if isinstance(node, exp.Literal):
        return hash_literal(node)
    return node

def parse(sql, database, schema, include_comments):
    try:
        pt = parse_one(sql.encode().decode('unicode_escape'), dialect='snowflake')
        pt = optimizer.qualify.qualify(pt,
                                     catalog=database,
                                     db=schema,
                                     dialect="snowflake",
                                     quote_identifiers=False,
                                     qualify_columns=False,
                                     validate_qualify_columns=False,
                                     expand_alias_refs=False,
                                     identify=False)
    except:
        return "parse_error"
    try:
        npt = pt.transform(transform, database, schema)
    except:
        return "transform_error"
    try:
        return npt.sql(dialect='snowflake', normalize=True, normalize_functions=True, comments=include_comments)
    except:
        return "generate_error"

def hash_literal(lit):
    if lit.is_string:
        v = hashlib.md5(lit.this.encode()).hexdigest()
    else:
        random.seed(lit.this)
        v = random.randint(0, 1000000000)
    l = exp.Literal(this=v, is_string=lit.is_string)
    if hasattr(lit, 'comments') and lit.comments is not None:
        l.comments = lit.comments
    if hasattr(lit, '_comments'):
        l._comments = lit._comments
    return l
$$;


-- sp to create view reporting.enriched_query_history
CREATE OR REPLACE PROCEDURE INTERNAL.create_view_enriched_query_history_normalized()
    RETURNS STRING
    LANGUAGE SQL
AS
BEGIN
    create or replace view reporting.enriched_query_history_normalized
    COPY GRANTS
    AS
        select * exclude (query_text), tools.normalize(query_text, database_name, schema_name) as query_text from reporting.enriched_query_history
        ;
    grant select on reporting.enriched_query_history_normalized to application role admin;
    grant select on reporting.enriched_query_history_normalized to application role read_only;
    RETURN 'Success';
END;

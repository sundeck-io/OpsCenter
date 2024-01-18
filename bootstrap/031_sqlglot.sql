
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
imports=('{{stage}}/python/sqlglot.zip')
handler='parse'
as
$$
from sqlglot import parse_one
import sqlglot.expressions as exp
def transform(node, database, schema):
    if schema is None:
        schema = ''
    if database is None:
        database = ''
    if isinstance(node, exp.Table) and node.name != '':
        tbl = [node.catalog or database, node.db or schema, node.name]
        tbl_str = '.'.join(i for i in tbl if i)
        if node.alias:
            tbl_str += ' ' + node.alias
        if hasattr(node, '_comments'):
            tbl_str += ' ' + node._comments
        return parse_one(tbl_str, dialect='snowflake', error_level='IGNORE')
    if isinstance(node, exp.Literal):
        l = exp.Literal(this='xxx', is_string=True) if node.is_string else exp.Literal(this=999, is_string=False)
        if hasattr(node, '_comments'):
            l._comments = node._comments
        return l
    return node
def parse(sql, database, schema, include_comments):
    try:
        pt = parse_one(sql.encode().decode('unicode_escape'), dialect='snowflake', error_level='IGNORE')
    except:
        return "parse_error"
    try:
        npt = pt.transform(transform, database, schema)
    except:
        return "transform_error"
    try:
        return npt.sql(dialect='snowflake', normalize=True, normalize_functions=True, comments=include_comments, unsupported_level='IGNORE')
    except:
        return "generate_error"

$$;

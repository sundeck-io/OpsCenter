import uuid


def generate_unique_name(prefix, timestamp_string) -> str:
    name = prefix + "_" + str(uuid.uuid4()) + "_" + timestamp_string
    return name


def _get_single_value(conn, sql):
    with conn() as cnx:
        cur = cnx.cursor()
        result = cur.execute(sql).fetchone()
        return result[0]


def run_proc(conn, sql) -> str:
    assert isinstance(result := _get_single_value(conn, sql), (str, type(None)))
    return result


def row_count(conn, sql) -> int:
    assert isinstance(result := _get_single_value(conn, sql), int)
    return result


def run_sql(conn, sql) -> str:
    result = _get_single_value(conn, sql)
    return str(result)


def delete_list_of_labels(conn, sql):
    print(f"[INFO] SQL in delete function: {sql}")

    with conn() as cnx:
        cur = cnx.cursor()
        for name in cur.execute(sql).fetchall():
            delete_label_statement = f"call ADMIN.DELETE_LABEL('{name[0]}');"
            assert "done" in str(
                run_proc(conn, delete_label_statement)
            ), "Stored procedure output does not match expected result!"


def delete_list_of_probes(conn, sql):
    print(f"[INFO] SQL in delete function: {sql}")

    with conn() as cnx:
        cur = cnx.cursor()
        for name in cur.execute(sql).fetchall():
            delete_probe_statement = f"call ADMIN.DELETE_PROBE('{name[0]}');"
            assert "done" in str(
                run_proc(conn, delete_probe_statement)
            ), "Stored procedure output does not match expected result!"

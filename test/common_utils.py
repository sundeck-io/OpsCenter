import uuid


def generate_unique_name(prefix, timestamp_string) -> str:
    name = prefix + "_" + str(uuid.uuid4()) + "_" + timestamp_string
    return name


def run_proc(conn_cnx, sql) -> str:
    with conn_cnx() as cnx:
        cur = cnx.cursor()
        result = cur.execute(sql).fetchone()
        return result


def delete_list_of_labels(conn_cnx, sql):
    print(f"[INFO] SQL in delete function: {sql}")

    with conn_cnx() as cnx:
        cur = cnx.cursor()
        for name in cur.execute(sql).fetchall():
            delete_label_statement = f"call ADMIN.DELETE_LABEL('{name[0]}');"
            assert "done" in str(
                run_proc(conn_cnx, delete_label_statement)
            ), "Stored procedure output does not match expected result!"


# Used for validating result correctness
def validate_row_count(conn_cnx, sql) -> int:
    with conn_cnx() as cnx:
        cur = cnx.cursor()
        result = cur.execute(sql).fetchone()
        return result

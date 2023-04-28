
CREATE OR REPLACE PROCEDURE admin.run_as_app(sql string)
RETURNS table()
language sql
execute as owner
AS
BEGIN
    let rs resultset := (execute immediate sql);
    return table(rs);
END;

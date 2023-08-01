# OpsCenter SQL tests

## Running tests

Place the `parameters.py` file in the `tests/sql` directory, with the connection information:

```python
CONNECTION_PARAMETERS = {
    'account':  'account',
    'user':     'user',
    'password': 'passwd',
    'schema':   'schema',
    'database': 'testdb',
}
```

### Running a single test

`python -m pytest sql/test_proc_create_label.py -s -v`.

### Running a single test function in the test

`python -m pytest sql/test_label_procs.py::test_smoke_create_drop_label -s`

### Running all the tests

`python -m pytest sql

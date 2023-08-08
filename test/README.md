# OpsCenter SQL tests

## Running tests

Connection information will be taken from ~/.snowsql/config file.
See example below:

```
[connections.local_dev]
accountname=xyz12345
username="vicky"
password="***"
warehousename ="COMPUTE_WH"
dbname = "testdb"
```

### Running a single test

`python -m pytest unit/test_label_procs.py -s -v --profile local_dev`.

### Running a single test function in the test

`python -m pytest unit/test_label_procs.py::test_smoke_create_drop_label -s --profile local_dev`

### Running all the unit tests

`python -m pytest unit --profile local_dev`

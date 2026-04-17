# rdb2graph — SQLite Connector Plugin
SQLite 3.x | Uses Python stdlib sqlite3 — no extra packages needed | Path = file path

## Install
No installation needed — sqlite3 is part of Python's standard library.

## config.yaml
```yaml
source_db:
  type: "sqlite"
  database: "/absolute/path/to/mydb.sqlite3"
  # host, port, user, password are ignored
```

## Known limitations
- SQLite does not name FK constraints — `fk_constraint_name` will always be null
- UNIQUE constraints defined in CREATE TABLE are not always detected; index-based uniques are skipped
- No schema support (SQLite has no schema concept)

## Contributing
Open a PR — see the root [README contributing guide](../../../../README.md#contributing).

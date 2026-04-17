# rdb2graph — dbdiagram.io DBML Parser Plugin
Parses DBML (Database Markup Language) files exported from dbdiagram.io.

Export from dbdiagram.io: **File → Export → Export to DBML** → save as `.dbml`

## Install
No extra packages needed (uses regex + stdlib only).

## config.yaml
```yaml
er_diagram:
  path: "./schema.dbml"
  format: "dbdiagram"
```

## DBML syntax supported
- `Table name { ... }` entity blocks
- Column definitions with `[pk]`, `[not null]`, `[unique]` settings
- `Ref:` relationship declarations with `<`, `>`, `-`, `<>` cardinality operators
- `//` and `/* */` comments

## Known limitations
- `Note` blocks are parsed but not stored in the model
- `Indexes` blocks are ignored (index info comes from column flags)
- Multi-schema DBML not yet supported

## Contributing
Open a PR — see the root [README contributing guide](../../../../README.md#contributing).

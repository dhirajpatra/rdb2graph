# rdb2graph — Oracle Database Connector Plugin
Oracle 12c+, 19c, 21c, 23ai | Driver: python-oracledb (thin mode) | Port default: 1521

## Install
```bash
pip install oracledb>=1.0.0
# Thin mode requires NO Oracle Client installation
```

## config.yaml
```yaml
source_db:
  type: "oracle"
  host: "localhost"
  port: 1521
  database: "ORCL"        # service name or SID
  user: "myuser"
  password: "secret"
  schema: "MYSCHEMA"      # defaults to user (uppercase)
  mode: "thin"            # thin (default) | thick
```

## Known limitations
- Schema name is case-sensitive in Oracle (stored uppercase)
- XMLTYPE columns mapped to TEXT
- Thick mode requires Oracle Client — set `mode: thick` and follow oracledb docs

## Contributing
Open a PR — see the root [README contributing guide](../../../../README.md#contributing).

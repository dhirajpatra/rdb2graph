# rdb2graph — Microsoft SQL Server Connector Plugin
SQL Server 2014+, Azure SQL | Driver: pyodbc + ODBC Driver 17/18 | Port default: 1433

## Install
```bash
pip install pyodbc>=4.0.0
# Also install the ODBC driver: https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server
```

## config.yaml
```yaml
source_db:
  type: "mssql"
  host: "localhost"
  port: 1433
  database: "mydb"
  user: "sa"
  password: "secret"
  schema: "dbo"
  driver: "ODBC Driver 18 for SQL Server"   # optional
```

## Known limitations
- Requires the Microsoft ODBC Driver for SQL Server to be installed on the OS
- Windows auth (`Trusted_Connection=yes`) not yet supported — PRs welcome

## Contributing
Open a PR — see the root [README contributing guide](../../../../README.md#contributing).

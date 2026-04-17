# rdb2graph — MySQL Connector Plugin
MySQL 5.7+ and MySQL 8.x | Driver: mysql-connector-python | Port default: 3306

## Install
```bash
pip install mysql-connector-python>=8.0.0
```

## config.yaml
```yaml
source_db:
  type: "mysql"
  host: "localhost"
  port: 3306
  database: "mydb"
  user: "root"
  password: "secret"
```

## Known limitations
- No schema filtering (MySQL uses database as the schema boundary)
- ENUM columns mapped to VARCHAR

## Contributing
Open a PR — see the root [README contributing guide](../../../../README.md#contributing).

"""
Microsoft SQL Server Connector Plugin for rdb2graph
Supports SQL Server 2014+, Azure SQL.

Install:  pip install pyodbc>=4.0.0
          (also requires ODBC Driver 17 or 18 for SQL Server)

config.yaml:
    source_db:
      type: "mssql"
      host: "localhost"
      port: 1433
      database: "mydb"
      user: "sa"
      password: "secret"
      driver: "ODBC Driver 18 for SQL Server"   # optional, default shown
      schema: "dbo"
"""
import logging
logger = logging.getLogger(__name__)

try:
    from src.schema_extractor import SchemaExtractorBase
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))
    from schema_extractor import SchemaExtractorBase  # type: ignore


class MSSQLExtractor(SchemaExtractorBase):
    PLUGIN_NAME = "mssql"
    REQUIRED_PACKAGES = ["pyodbc>=4.0.0"]

    def __init__(self, config: dict):
        self.config = config["source_db"]
        self.schema = self.config.get("schema", "dbo")
        self._conn = None

    def connect(self):
        try:
            import pyodbc
        except ImportError:
            raise ImportError("Run: pip install pyodbc  (and install the ODBC driver)")
        driver = self.config.get("driver", "ODBC Driver 18 for SQL Server")
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={self.config['host']},{self.config.get('port', 1433)};"
            f"DATABASE={self.config['database']};"
            f"UID={self.config['user']};PWD={self.config['password']};"
            "TrustServerCertificate=yes;"
        )
        self._conn = pyodbc.connect(conn_str)
        logger.info(f"MSSQL connected: {self.config['host']}/{self.config['database']}")

    def disconnect(self):
        if self._conn:
            self._conn.close()

    def extract(self) -> dict:
        schema = self.schema
        cur = self._conn.cursor()

        cur.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME",
            (schema,)
        )
        tables = [r[0] for r in cur.fetchall()]

        cur.execute(
            "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,ORDINAL_POSITION "
            "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? ORDER BY TABLE_NAME,ORDINAL_POSITION",
            (schema,)
        )
        col_rows = cur.fetchall()

        cur.execute(
            "SELECT kcu.TABLE_NAME,kcu.COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
            "JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON kcu.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
            "WHERE tc.CONSTRAINT_TYPE='PRIMARY KEY' AND kcu.TABLE_SCHEMA=?",
            (schema,)
        )
        pk_set = {(r[0], r[1]) for r in cur.fetchall()}

        cur.execute(
            "SELECT kcu.TABLE_NAME,kcu.COLUMN_NAME,ccu.TABLE_NAME AS REF_TABLE,"
            "ccu.COLUMN_NAME AS REF_COL,tc.CONSTRAINT_NAME "
            "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
            "JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ON kcu.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
            "JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME=ccu.CONSTRAINT_NAME "
            "WHERE tc.CONSTRAINT_TYPE='FOREIGN KEY' AND kcu.TABLE_SCHEMA=?",
            (schema,)
        )
        fk_map = {(r[0], r[1]): (r[2], r[3], r[4]) for r in cur.fetchall()}
        cur.close()

        result = {t: [] for t in tables}
        for tbl, col, dtype, nullable, default, ordinal in col_rows:
            if tbl not in result: continue
            fk = fk_map.get((tbl, col))
            result[tbl].append({"column_name": col, "data_type": dtype.upper(),
                "is_primary_key": (tbl, col) in pk_set, "is_nullable": nullable == "YES",
                "is_unique": False, "default_value": default, "ordinal_position": ordinal,
                "foreign_table": fk[0] if fk else None, "foreign_column": fk[1] if fk else None,
                "fk_constraint_name": fk[2] if fk else None})
        logger.info(f"MSSQL: {len(result)} tables, {sum(len(v) for v in result.values())} columns")
        return result

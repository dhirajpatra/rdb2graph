"""
MySQL Database Connector Plugin for rdb2graph
Supports MySQL 5.7+ and MySQL 8.x.

Install:  pip install mysql-connector-python>=8.0.0

config.yaml:
    source_db:
      type: "mysql"
      host: "localhost"
      port: 3306
      database: "mydb"
      user: "root"
      password: "secret"
"""
import logging
logger = logging.getLogger(__name__)

try:
    from src.schema_extractor import SchemaExtractorBase
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))
    from schema_extractor import SchemaExtractorBase  # type: ignore


class MySQLExtractor(SchemaExtractorBase):
    PLUGIN_NAME = "mysql"
    REQUIRED_PACKAGES = ["mysql-connector-python>=8.0.0"]

    def __init__(self, config: dict):
        self.config = config["source_db"]
        self._conn = None

    def connect(self):
        try:
            import mysql.connector
        except ImportError:
            raise ImportError("Run: pip install mysql-connector-python")
        self._conn = mysql.connector.connect(
            host=self.config["host"], port=int(self.config.get("port", 3306)),
            database=self.config["database"], user=self.config["user"],
            password=self.config["password"], use_pure=True,
        )
        logger.info(f"MySQL connected: {self.config['host']}/{self.config['database']}")

    def disconnect(self):
        if self._conn and self._conn.is_connected():
            self._conn.close()

    def extract(self) -> dict:
        db = self.config["database"]
        cur = self._conn.cursor(dictionary=True)

        cur.execute("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA=%s AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", (db,))
        tables = [r["TABLE_NAME"] for r in cur.fetchall()]

        cur.execute("SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COLUMN_DEFAULT,ORDINAL_POSITION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s ORDER BY TABLE_NAME,ORDINAL_POSITION", (db,))
        col_rows = cur.fetchall()

        cur.execute("SELECT k.TABLE_NAME,k.COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE k JOIN information_schema.TABLE_CONSTRAINTS t ON k.CONSTRAINT_NAME=t.CONSTRAINT_NAME AND k.TABLE_SCHEMA=t.TABLE_SCHEMA WHERE t.CONSTRAINT_TYPE='PRIMARY KEY' AND k.TABLE_SCHEMA=%s", (db,))
        pk_set = {(r["TABLE_NAME"], r["COLUMN_NAME"]) for r in cur.fetchall()}

        cur.execute("SELECT k.TABLE_NAME,k.COLUMN_NAME,k.REFERENCED_TABLE_NAME,k.REFERENCED_COLUMN_NAME,k.CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE k WHERE k.TABLE_SCHEMA=%s AND k.REFERENCED_TABLE_NAME IS NOT NULL", (db,))
        fk_map = {(r["TABLE_NAME"], r["COLUMN_NAME"]): (r["REFERENCED_TABLE_NAME"], r["REFERENCED_COLUMN_NAME"], r["CONSTRAINT_NAME"]) for r in cur.fetchall()}
        cur.close()

        schema = {t: [] for t in tables}
        for row in col_rows:
            tbl, col = row["TABLE_NAME"], row["COLUMN_NAME"]
            if tbl not in schema: continue
            fk = fk_map.get((tbl, col))
            schema[tbl].append({"column_name": col, "data_type": row["DATA_TYPE"].upper(),
                "is_primary_key": (tbl, col) in pk_set, "is_nullable": row["IS_NULLABLE"] == "YES",
                "is_unique": False, "default_value": row["COLUMN_DEFAULT"],
                "ordinal_position": row["ORDINAL_POSITION"],
                "foreign_table": fk[0] if fk else None, "foreign_column": fk[1] if fk else None,
                "fk_constraint_name": fk[2] if fk else None})
        logger.info(f"MySQL: {len(schema)} tables, {sum(len(v) for v in schema.values())} columns")
        return schema

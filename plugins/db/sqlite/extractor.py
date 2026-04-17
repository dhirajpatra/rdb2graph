"""
SQLite Database Connector Plugin for rdb2graph
Supports SQLite 3.x. Uses stdlib only — no extra packages needed.

config.yaml:
    source_db:
      type: "sqlite"
      database: "/path/to/mydb.sqlite3"   # absolute or relative path
      # host, port, user, password are ignored for SQLite
"""
import logging
import sqlite3
import re
logger = logging.getLogger(__name__)

try:
    from src.schema_extractor import SchemaExtractorBase
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))
    from schema_extractor import SchemaExtractorBase  # type: ignore

# SQLite affinity → standard type
SQLITE_TYPE_MAP = {
    "INTEGER": "INTEGER", "INT": "INTEGER", "TINYINT": "INTEGER",
    "SMALLINT": "INTEGER", "MEDIUMINT": "INTEGER", "BIGINT": "BIGINT",
    "REAL": "FLOAT", "FLOAT": "FLOAT", "DOUBLE": "DOUBLE", "NUMERIC": "NUMERIC",
    "DECIMAL": "DECIMAL", "TEXT": "TEXT", "VARCHAR": "VARCHAR", "CHAR": "CHAR",
    "BLOB": "BLOB", "BOOLEAN": "BOOLEAN", "DATE": "DATE",
    "DATETIME": "DATETIME", "TIMESTAMP": "TIMESTAMP",
}


class SQLiteExtractor(SchemaExtractorBase):
    PLUGIN_NAME = "sqlite"
    REQUIRED_PACKAGES = []   # stdlib only

    def __init__(self, config: dict):
        self.config = config["source_db"]
        self._conn = None

    def connect(self):
        db_path = self.config["database"]
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        logger.info(f"SQLite connected: {db_path}")

    def disconnect(self):
        if self._conn:
            self._conn.close()

    def extract(self) -> dict:
        cur = self._conn.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        tables = [r[0] for r in cur.fetchall()]
        logger.info(f"SQLite: found {len(tables)} tables")

        result = {}
        for tbl in tables:
            # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            cur.execute(f"PRAGMA table_info('{tbl}')")
            col_infos = cur.fetchall()

            # PRAGMA foreign_key_list returns: id, seq, table, from, to, on_update, on_delete, match
            cur.execute(f"PRAGMA foreign_key_list('{tbl}')")
            fk_rows = {r["from"]: (r["table"], r["to"]) for r in cur.fetchall()}

            cols = []
            for info in col_infos:
                col_name = info["name"]
                raw_type = (info["type"] or "TEXT").upper().split("(")[0].strip()
                dtype = SQLITE_TYPE_MAP.get(raw_type, raw_type)
                fk = fk_rows.get(col_name)
                cols.append({
                    "column_name": col_name,
                    "data_type": dtype,
                    "is_primary_key": bool(info["pk"]),
                    "is_nullable": not bool(info["notnull"]),
                    "is_unique": False,
                    "default_value": info["dflt_value"],
                    "ordinal_position": info["cid"] + 1,
                    "foreign_table": fk[0] if fk else None,
                    "foreign_column": fk[1] if fk else None,
                    "fk_constraint_name": None,   # SQLite doesn't name FK constraints
                })
            result[tbl] = cols

        cur.close()
        logger.info(f"SQLite: {len(result)} tables, {sum(len(v) for v in result.values())} columns")
        return result

"""
Oracle Database Connector Plugin for rdb2graph
Supports Oracle 12c+, Oracle 19c, Oracle 21c, Oracle 23ai.

Install:  pip install oracledb>=1.0.0
          (python-oracledb — the modern Oracle driver, no Oracle Client needed in thin mode)

config.yaml:
    source_db:
      type: "oracle"
      host: "localhost"
      port: 1521
      database: "ORCL"       # service name or SID
      user: "myuser"
      password: "secret"
      schema: "MYSCHEMA"     # defaults to user (uppercase)
      mode: "thin"           # thin (default) | thick
"""
import logging
logger = logging.getLogger(__name__)

try:
    from src.schema_extractor import SchemaExtractorBase
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))
    from schema_extractor import SchemaExtractorBase  # type: ignore


# Oracle type → standard uppercase type
ORACLE_TYPE_MAP = {
    "NUMBER": "NUMERIC", "VARCHAR2": "VARCHAR", "NVARCHAR2": "VARCHAR",
    "CHAR": "CHAR", "NCHAR": "CHAR", "CLOB": "TEXT", "NCLOB": "TEXT",
    "BLOB": "BLOB", "DATE": "DATE", "TIMESTAMP(6)": "TIMESTAMP",
    "FLOAT": "FLOAT", "BINARY_FLOAT": "FLOAT", "BINARY_DOUBLE": "DOUBLE",
    "RAW": "BINARY", "LONG": "TEXT", "XMLTYPE": "TEXT",
}


class OracleExtractor(SchemaExtractorBase):
    PLUGIN_NAME = "oracle"
    REQUIRED_PACKAGES = ["oracledb>=1.0.0"]

    def __init__(self, config: dict):
        self.config = config["source_db"]
        self.schema = (self.config.get("schema") or self.config["user"]).upper()
        self._conn = None

    def connect(self):
        try:
            import oracledb
        except ImportError:
            raise ImportError("Run: pip install oracledb")
        mode = self.config.get("mode", "thin")
        if mode == "thin":
            oracledb.init_oracle_client()  # no-op in thin mode
        dsn = f"{self.config['host']}:{self.config.get('port', 1521)}/{self.config['database']}"
        self._conn = oracledb.connect(user=self.config["user"], password=self.config["password"], dsn=dsn)
        logger.info(f"Oracle connected: {dsn} (schema={self.schema})")

    def disconnect(self):
        if self._conn:
            self._conn.close()

    def extract(self) -> dict:
        cur = self._conn.cursor()
        schema = self.schema

        cur.execute("SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER=:s ORDER BY TABLE_NAME", {"s": schema})
        tables = [r[0] for r in cur.fetchall()]

        cur.execute(
            "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,NULLABLE,DATA_DEFAULT,COLUMN_ID "
            "FROM ALL_TAB_COLUMNS WHERE OWNER=:s ORDER BY TABLE_NAME,COLUMN_ID",
            {"s": schema}
        )
        col_rows = cur.fetchall()

        cur.execute(
            "SELECT cols.TABLE_NAME,cols.COLUMN_NAME FROM ALL_CONSTRAINTS cons "
            "JOIN ALL_CONS_COLUMNS cols ON cons.CONSTRAINT_NAME=cols.CONSTRAINT_NAME AND cons.OWNER=cols.OWNER "
            "WHERE cons.CONSTRAINT_TYPE='P' AND cons.OWNER=:s",
            {"s": schema}
        )
        pk_set = {(r[0], r[1]) for r in cur.fetchall()}

        cur.execute(
            "SELECT a.TABLE_NAME,a.COLUMN_NAME,c_pk.TABLE_NAME AS REF_TABLE,"
            "b.COLUMN_NAME AS REF_COL,a.CONSTRAINT_NAME "
            "FROM ALL_CONS_COLUMNS a "
            "JOIN ALL_CONSTRAINTS c ON a.OWNER=c.OWNER AND a.CONSTRAINT_NAME=c.CONSTRAINT_NAME "
            "JOIN ALL_CONSTRAINTS c_pk ON c.R_OWNER=c_pk.OWNER AND c.R_CONSTRAINT_NAME=c_pk.CONSTRAINT_NAME "
            "JOIN ALL_CONS_COLUMNS b ON c_pk.OWNER=b.OWNER AND c_pk.CONSTRAINT_NAME=b.CONSTRAINT_NAME "
            "WHERE c.CONSTRAINT_TYPE='R' AND a.OWNER=:s",
            {"s": schema}
        )
        fk_map = {(r[0], r[1]): (r[2], r[3], r[4]) for r in cur.fetchall()}
        cur.close()

        result = {t: [] for t in tables}
        for tbl, col, dtype, nullable, default, col_id in col_rows:
            if tbl not in result: continue
            fk = fk_map.get((tbl, col))
            result[tbl].append({"column_name": col,
                "data_type": ORACLE_TYPE_MAP.get(dtype.upper(), dtype.upper()),
                "is_primary_key": (tbl, col) in pk_set, "is_nullable": nullable == "Y",
                "is_unique": False, "default_value": str(default).strip() if default else None,
                "ordinal_position": col_id,
                "foreign_table": fk[0] if fk else None, "foreign_column": fk[1] if fk else None,
                "fk_constraint_name": fk[2] if fk else None})
        logger.info(f"Oracle: {len(result)} tables, {sum(len(v) for v in result.values())} columns")
        return result

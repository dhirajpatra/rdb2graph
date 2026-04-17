"""
Schema Extractor — PostgreSQL (primary), extensible to MySQL/MSSQL/Oracle
Introspects live DB: tables, columns, types, PKs, FKs, constraints
"""

import logging
from typing import Optional
logger = logging.getLogger(__name__)


class SchemaExtractor:
    """
    Extracts full schema from relational DB.
    Returns dict: {table_name: [column_dicts]}
    Each column_dict: {
        column_name, data_type, is_primary_key, is_nullable,
        is_unique, default_value, foreign_table, foreign_column, ordinal_position
    }
    """

    def __init__(self, config: dict):
        self.config = config["source_db"]
        self.schema = self.config.get("schema", "public")
        self._conn = None

    def connect(self):
        db_type = self.config["type"].lower()
        if db_type == "postgresql":
            import psycopg2
            self._conn = psycopg2.connect(
                host=self.config["host"],
                port=self.config["port"],
                dbname=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
            )
            logger.info(f"Connected to PostgreSQL: {self.config['host']}:{self.config['port']}/{self.config['database']}")
        elif db_type == "mysql":
            import mysql.connector
            self._conn = mysql.connector.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
            )
            logger.info(f"Connected to MySQL: {self.config['host']}")
        else:
            # Generic SQLAlchemy fallback
            from sqlalchemy import create_engine
            url = f"{db_type}://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            engine = create_engine(url)
            self._conn = engine.connect()
            logger.info(f"Connected via SQLAlchemy: {db_type}")

    def disconnect(self):
        if self._conn:
            self._conn.close()
            logger.info("DB connection closed")

    def extract(self) -> dict:
        """Main entry point — returns {table_name: [columns]}"""
        if not self._conn:
            self.connect()
        db_type = self.config["type"].lower()
        if db_type == "postgresql":
            return self._extract_postgresql()
        elif db_type == "mysql":
            return self._extract_mysql()
        else:
            logger.warning(f"DB type '{db_type}' using generic SQLAlchemy extraction")
            return self._extract_sqlalchemy()

    # ── PostgreSQL ────────────────────────────────────────────────────────────

    def _extract_postgresql(self) -> dict:
        cursor = self._conn.cursor()
        schema = self.schema

        # 1. All tables in schema
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """, (schema,))
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables in schema '{schema}'")

        # 2. All columns with full metadata
        cursor.execute("""
            SELECT
                c.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.ordinal_position,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale
            FROM information_schema.columns c
            WHERE c.table_schema = %s
            ORDER BY c.table_name, c.ordinal_position
        """, (schema,))
        col_rows = cursor.fetchall()

        # 3. Primary keys
        cursor.execute("""
            SELECT kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
        """, (schema,))
        pk_set = {(r[0], r[1]) for r in cursor.fetchall()}

        # 4. Unique constraints
        cursor.execute("""
            SELECT kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
              AND tc.table_schema = %s
        """, (schema,))
        unique_set = {(r[0], r[1]) for r in cursor.fetchall()}

        # 5. Foreign keys
        cursor.execute("""
            SELECT
                kcu.table_name AS from_table,
                kcu.column_name AS from_column,
                ccu.table_name AS to_table,
                ccu.column_name AS to_column,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = %s
        """, (schema,))
        fk_map = {}  # (from_table, from_col) → (to_table, to_col, constraint_name)
        for from_tbl, from_col, to_tbl, to_col, constraint_name in cursor.fetchall():
            fk_map[(from_tbl, from_col)] = (to_tbl, to_col, constraint_name)

        cursor.close()

        # Assemble result
        schema_dict = {t: [] for t in tables}
        for row in col_rows:
            tbl, col, dtype, nullable, default, ordinal, max_len, num_prec, num_scale = row
            if tbl not in schema_dict:
                continue
            fk_info = fk_map.get((tbl, col))
            schema_dict[tbl].append({
                "column_name": col,
                "data_type": dtype.upper(),
                "is_primary_key": (tbl, col) in pk_set,
                "is_nullable": nullable == "YES",
                "is_unique": (tbl, col) in unique_set,
                "default_value": default,
                "ordinal_position": ordinal,
                "max_length": max_len,
                "numeric_precision": num_prec,
                "numeric_scale": num_scale,
                "foreign_table": fk_info[0] if fk_info else None,
                "foreign_column": fk_info[1] if fk_info else None,
                "fk_constraint_name": fk_info[2] if fk_info else None,
            })

        logger.info(f"Schema extracted: {sum(len(v) for v in schema_dict.values())} total columns across {len(schema_dict)} tables")
        return schema_dict

    # ── MySQL ─────────────────────────────────────────────────────────────────

    def _extract_mysql(self) -> dict:
        """MySQL schema extraction — same structure as PG output"""
        cursor = self._conn.cursor(dictionary=True)
        db = self.config["database"]

        cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema='{db}' AND table_type='BASE TABLE'")
        tables = [r["TABLE_NAME"] for r in cursor.fetchall()]

        cursor.execute(f"""
            SELECT table_name, column_name, data_type, is_nullable, column_default, ordinal_position
            FROM information_schema.columns WHERE table_schema='{db}' ORDER BY table_name, ordinal_position
        """)
        col_rows = cursor.fetchall()

        cursor.execute(f"""
            SELECT k.table_name, k.column_name FROM information_schema.key_column_usage k
            JOIN information_schema.table_constraints t
              ON k.constraint_name = t.constraint_name AND k.table_schema = t.table_schema
            WHERE t.constraint_type='PRIMARY KEY' AND k.table_schema='{db}'
        """)
        pk_set = {(r["TABLE_NAME"], r["COLUMN_NAME"]) for r in cursor.fetchall()}

        cursor.execute(f"""
            SELECT k.table_name, k.column_name, k.referenced_table_name, k.referenced_column_name
            FROM information_schema.key_column_usage k
            WHERE k.table_schema='{db}' AND k.referenced_table_name IS NOT NULL
        """)
        fk_map = {(r["TABLE_NAME"], r["COLUMN_NAME"]): (r["REFERENCED_TABLE_NAME"], r["REFERENCED_COLUMN_NAME"], None)
                  for r in cursor.fetchall()}
        cursor.close()

        schema_dict = {t: [] for t in tables}
        for row in col_rows:
            tbl, col = row["TABLE_NAME"], row["COLUMN_NAME"]
            if tbl not in schema_dict:
                continue
            fk_info = fk_map.get((tbl, col))
            schema_dict[tbl].append({
                "column_name": col,
                "data_type": row["DATA_TYPE"].upper(),
                "is_primary_key": (tbl, col) in pk_set,
                "is_nullable": row["IS_NULLABLE"] == "YES",
                "is_unique": False,
                "default_value": row["COLUMN_DEFAULT"],
                "ordinal_position": row["ORDINAL_POSITION"],
                "foreign_table": fk_info[0] if fk_info else None,
                "foreign_column": fk_info[1] if fk_info else None,
                "fk_constraint_name": fk_info[2] if fk_info else None,
            })
        return schema_dict

    # ── SQLAlchemy generic fallback ────────────────────────────────────────────

    def _extract_sqlalchemy(self) -> dict:
        from sqlalchemy import inspect
        insp = inspect(self._conn)
        schema_dict = {}
        for tbl in insp.get_table_names():
            pks = set(insp.get_pk_constraint(tbl).get("constrained_columns", []))
            fks = {fk["constrained_columns"][0]: (fk["referred_table"], fk["referred_columns"][0])
                   for fk in insp.get_foreign_keys(tbl) if fk["constrained_columns"]}
            cols = []
            for c in insp.get_columns(tbl):
                fk_info = fks.get(c["name"])
                cols.append({
                    "column_name": c["name"],
                    "data_type": str(c["type"]).upper(),
                    "is_primary_key": c["name"] in pks,
                    "is_nullable": c.get("nullable", True),
                    "is_unique": False,
                    "default_value": str(c.get("default", "")),
                    "ordinal_position": 0,
                    "foreign_table": fk_info[0] if fk_info else None,
                    "foreign_column": fk_info[1] if fk_info else None,
                    "fk_constraint_name": None,
                })
            schema_dict[tbl] = cols
        return schema_dict

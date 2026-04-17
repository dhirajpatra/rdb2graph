"""
dbdiagram.io ER Diagram Parser Plugin for rdb2graph
Parses the DBML (Database Markup Language) format exported by dbdiagram.io.

dbdiagram.io export: File → Export → Export to DBML  →  save as .dbml

Install:  No extra packages needed (regex + stdlib only).
          Optional: pip install pydbml>=1.0.0  (for richer DBML support)

config.yaml:
    er_diagram:
      path: "./schema.dbml"
      format: "dbdiagram"
"""
import re
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from src.er_parser import ERParserBase, ERModel, ERTable, ERColumn, ERRelationship, to_pascal_case
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))
    from er_parser import ERParserBase, ERModel, ERTable, ERColumn, ERRelationship, to_pascal_case  # type: ignore


# DBML type → standard type
DBML_TYPE_MAP = {
    "int": "INTEGER", "integer": "INTEGER", "bigint": "BIGINT", "smallint": "SMALLINT",
    "float": "FLOAT", "double": "DOUBLE", "decimal": "DECIMAL", "numeric": "NUMERIC",
    "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "varchar": "VARCHAR", "text": "TEXT", "char": "CHAR", "uuid": "UUID",
    "date": "DATE", "datetime": "DATETIME", "timestamp": "TIMESTAMP", "time": "TIME",
    "json": "JSON", "jsonb": "JSON",
}

# DBML relationship cardinality symbols
CARD_MAP = {
    "<": "one-to-many",   # one-to-many  (left one, right many)
    ">": "many-to-one",   # many-to-one
    "-": "one-to-one",
    "<>": "many-to-many",
}


class DbdiagramParser(ERParserBase):
    """
    Parses DBML files exported from dbdiagram.io.

    DBML syntax example:
        Table users {
          id integer [pk]
          username varchar
          email varchar [not null, unique]
        }
        Ref: orders.user_id > users.id   // many-to-one
    """

    PLUGIN_NAME = "dbdiagram"
    FILE_EXTENSIONS = [".dbml", ".txt"]
    REQUIRED_PACKAGES = []

    def __init__(self, path: str):
        self.path = path

    def parse(self) -> ERModel:
        logger.info(f"Parsing dbdiagram.io DBML: {self.path}")
        text = Path(self.path).read_text(encoding="utf-8")
        # Strip comments
        text = re.sub(r"//[^\n]*", "", text)
        text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)

        model = ERModel(source_format="dbdiagram", source_path=self.path)
        model.tables = self._parse_tables(text)
        model.relationships = self._parse_refs(text, model.tables)

        logger.info(f"dbdiagram: {len(model.tables)} tables, {len(model.relationships)} relationships")
        return model

    # ── Tables ────────────────────────────────────────────────────────────────

    def _parse_tables(self, text: str) -> list:
        tables = []
        # Match:  Table <name> [as <alias>] { ... }
        for m in re.finditer(r'Table\s+"?(\w+)"?\s*(?:as\s+\w+\s*)?\{([^}]*)\}', text, re.IGNORECASE | re.DOTALL):
            name = m.group(1)
            body = m.group(2)
            table = ERTable(name=name, node_label=to_pascal_case(name))
            for line in body.strip().splitlines():
                col = self._parse_column(line.strip())
                if col:
                    table.columns.append(col)
                    if col.is_primary_key:
                        table.primary_keys.append(col.name)
            tables.append(table)
        return tables

    def _parse_column(self, line: str) -> Optional[ERColumn]:
        if not line or line.startswith("indexes") or line.startswith("Note"):
            return None
        # Pattern: col_name  data_type  [settings]
        m = re.match(r'"?(\w+)"?\s+(\w+)\s*(?:\[([^\]]*)\])?', line)
        if not m:
            return None
        col_name, raw_type, settings = m.group(1), m.group(2), (m.group(3) or "")
        dtype = DBML_TYPE_MAP.get(raw_type.lower(), raw_type.upper())
        settings_lower = settings.lower()
        return ERColumn(
            name=col_name,
            data_type=dtype,
            is_primary_key="pk" in settings_lower or "primary key" in settings_lower,
            is_nullable="not null" not in settings_lower,
            is_unique="unique" in settings_lower,
        )

    # ── Refs (relationships) ──────────────────────────────────────────────────

    def _parse_refs(self, text: str, tables: list) -> list:
        """
        DBML ref syntax:
            Ref ref_name: tableA.col  >  tableB.col    // many-to-one
            Ref: tableA.col  <  tableB.col             // one-to-many
            Ref: tableA.col  -  tableB.col             // one-to-one
            Ref: tableA.col  <>  tableB.col            // many-to-many
        """
        table_map = {t.name: t for t in tables}
        rels = []
        for m in re.finditer(
            r'Ref\s*\w*\s*:\s*"?(\w+)"?\."?(\w+)"?\s*(<>|<|>|-)\s*"?(\w+)"?\."?(\w+)"?',
            text, re.IGNORECASE
        ):
            from_table, from_col, op, to_table, to_col = m.groups()
            cardinality = CARD_MAP.get(op, "many-to-one")
            from_t = table_map.get(from_table)
            to_t = table_map.get(to_table)
            label = f"BELONGS_TO_{to_pascal_case(to_table).upper()}"
            rels.append(ERRelationship(
                name=f"ref_{from_table}_{from_col}",
                from_table=from_table,
                to_table=to_table,
                from_column=from_col,
                to_column=to_col,
                cardinality=cardinality,
                semantic_label=label,
                direction="OUTGOING",
            ))
            logger.debug(f"  Ref: ({from_table}.{from_col}) {op} ({to_table}.{to_col})  [{cardinality}]")
        return rels

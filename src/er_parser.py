"""
Stage 1 — ER Diagram Parser
Supports: MySQL Workbench .mwb (primary), extensible to DDL, dbdiagram JSON, image
.mwb is a ZIP containing document.mwb.xml — parsed with stdlib xml.etree
"""

import zipfile
import xml.etree.ElementTree as ET
import json
import logging
import re
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

logger = logging.getLogger(__name__)


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class ERColumn:
    name: str
    data_type: str
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_nullable: bool = True
    is_unique: bool = False
    default_value: Optional[str] = None
    comment: Optional[str] = None


@dataclass
class ERRelationship:
    name: str
    from_table: str
    to_table: str
    from_column: str
    to_column: str
    cardinality: str = "many-to-one"   # one-to-one | one-to-many | many-to-one | many-to-many
    # Semantic label derived from relationship name / comment
    semantic_label: str = ""
    direction: str = "OUTGOING"        # OUTGOING | INCOMING | UNDIRECTED


@dataclass
class ERTable:
    name: str
    columns: list = field(default_factory=list)
    primary_keys: list = field(default_factory=list)
    comment: Optional[str] = None
    # Suggested Neo4j node label (PascalCase)
    node_label: str = ""


@dataclass
class ERModel:
    tables: list = field(default_factory=list)
    relationships: list = field(default_factory=list)
    source_format: str = "mwb"
    source_path: str = ""


# ── Helpers ───────────────────────────────────────────────────────────────────

def to_pascal_case(name: str) -> str:
    """orders_items → OrdersItems, customer → Customer"""
    return "".join(word.capitalize() for word in re.split(r"[_\s]+", name))


def infer_relationship_label(rel_name: str, from_table: str, to_table: str) -> str:
    """
    Derive a meaningful Cypher relationship type.
    FK name 'fk_orders_customer_id' → 'BELONGS_TO'
    Generic fallback: 'HAS_<TO_TABLE>'
    """
    # Common FK naming patterns → semantic labels
    patterns = {
        r"(?:fk_)?(\w+)_(\w+)_id$": None,   # will use table names
        r"parent": "HAS_PARENT",
        r"owner": "OWNED_BY",
        r"author": "AUTHORED_BY",
        r"created_by": "CREATED_BY",
        r"assigned": "ASSIGNED_TO",
        r"belongs": "BELONGS_TO",
        r"member": "MEMBER_OF",
        r"contains": "CONTAINS",
        r"references": "REFERENCES",
    }
    lower = rel_name.lower()
    for pattern, label in patterns.items():
        if re.search(pattern, lower):
            if label:
                return label
    # Default: BELONGS_TO_{TO_TABLE} or HAS_{TO_TABLE}
    to_label = to_pascal_case(to_table).upper()
    from_label = to_pascal_case(from_table).upper()
    return f"BELONGS_TO_{to_label}" if "id" in rel_name.lower() else f"HAS_{to_label}"


def infer_cardinality(many_key: str, one_key: str) -> str:
    """Basic cardinality from MWB relationship attributes"""
    mapping = {
        ("1", "1"): "one-to-one",
        ("1", "n"): "one-to-many",
        ("n", "1"): "many-to-one",
        ("n", "n"): "many-to-many",
    }
    return mapping.get((many_key, one_key), "many-to-one")


# ── MWB Parser ────────────────────────────────────────────────────────────────

class MWBParser:
    """
    Parses MySQL Workbench .mwb files.
    .mwb = ZIP archive containing 'document.mwb.xml'
    Uses stdlib only: zipfile + xml.etree.ElementTree
    """

    # MWB XML namespaces
    NS = {
        "mwb": "http://www.mysql.com/wb-schema",
    }
    # Internal MWB type → canonical type
    TYPE_MAP = {
        "com.mysql.rdbms.mysql.datatype.int": "INTEGER",
        "com.mysql.rdbms.mysql.datatype.bigint": "BIGINT",
        "com.mysql.rdbms.mysql.datatype.varchar": "VARCHAR",
        "com.mysql.rdbms.mysql.datatype.text": "TEXT",
        "com.mysql.rdbms.mysql.datatype.datetime": "DATETIME",
        "com.mysql.rdbms.mysql.datatype.timestamp": "TIMESTAMP",
        "com.mysql.rdbms.mysql.datatype.date": "DATE",
        "com.mysql.rdbms.mysql.datatype.float": "FLOAT",
        "com.mysql.rdbms.mysql.datatype.double": "DOUBLE",
        "com.mysql.rdbms.mysql.datatype.decimal": "DECIMAL",
        "com.mysql.rdbms.mysql.datatype.boolean": "BOOLEAN",
        "com.mysql.rdbms.mysql.datatype.tinyint": "TINYINT",
        "com.mysql.rdbms.mysql.datatype.json": "JSON",
        "com.mysql.rdbms.mysql.datatype.uuid": "UUID",
    }

    def __init__(self, mwb_path: str):
        self.mwb_path = Path(mwb_path)
        self._column_id_map: dict[str, tuple[str, str]] = {}  # id → (table_name, col_name)
        self._table_id_map: dict[str, str] = {}               # id → table_name

    def parse(self) -> ERModel:
        logger.info(f"Parsing MWB file: {self.mwb_path}")
        xml_content = self._extract_xml()
        root = ET.fromstring(xml_content)
        model = ERModel(source_format="mwb", source_path=str(self.mwb_path))

        # MWB XML structure: root → value[@type='workbench.physical.Model']
        # → value[@type='db.mysql.Catalog'] → value[@type='db.mysql.Schema']
        # → value[@type='db.mysql.Table'] ...
        tables = self._find_tables(root)
        for tbl_el in tables:
            table = self._parse_table(tbl_el)
            if table:
                model.tables.append(table)
                logger.debug(f"  Table: {table.name} ({len(table.columns)} columns)")

        relationships = self._find_relationships(root)
        for rel_el in relationships:
            rel = self._parse_relationship(rel_el)
            if rel:
                model.relationships.append(rel)
                logger.debug(f"  Rel: {rel.from_table}.{rel.from_column} → {rel.to_table}.{rel.to_column} [{rel.semantic_label}]")

        logger.info(f"Parsed {len(model.tables)} tables, {len(model.relationships)} relationships")
        return model

    def _extract_xml(self) -> str:
        """Extract document.mwb.xml from the .mwb ZIP archive"""
        with zipfile.ZipFile(self.mwb_path, "r") as zf:
            names = zf.namelist()
            xml_file = next((n for n in names if n.endswith(".xml")), None)
            if not xml_file:
                raise ValueError(f"No XML found in {self.mwb_path}. Files: {names}")
            logger.debug(f"Extracting {xml_file} from ZIP")
            return zf.read(xml_file).decode("utf-8")

    def _find_tables(self, root: ET.Element) -> list:
        """Walk XML tree to find all db.mysql.Table elements"""
        tables = []
        for el in root.iter("value"):
            if el.get("type") in ("db.mysql.Table", "db.Table"):
                tables.append(el)
        logger.debug(f"Found {len(tables)} table elements in XML")
        return tables

    def _parse_table(self, tbl_el: ET.Element) -> Optional[ERTable]:
        name = self._get_attr(tbl_el, "name")
        if not name:
            return None

        table_id = tbl_el.get("id", "")
        self._table_id_map[table_id] = name

        comment = self._get_attr(tbl_el, "comment")
        table = ERTable(
            name=name,
            node_label=to_pascal_case(name),
            comment=comment,
        )

        # Parse columns
        pk_ids = set()
        for indices_el in tbl_el.iter("value"):
            if indices_el.get("type") in ("db.mysql.Index", "db.Index"):
                index_type = self._get_attr(indices_el, "indexType")
                if index_type == "PRIMARY":
                    for col_ref in indices_el.iter("value"):
                        ref_id = col_ref.get("key") or col_ref.text
                        if ref_id:
                            pk_ids.add(ref_id.strip())

        for col_el in tbl_el.iter("value"):
            if col_el.get("type") in ("db.mysql.Column", "db.Column"):
                col = self._parse_column(col_el, pk_ids)
                if col:
                    table.columns.append(col)
                    if col.is_primary_key:
                        table.primary_keys.append(col.name)
                    self._column_id_map[col_el.get("id", "")] = (name, col.name)

        return table

    def _parse_column(self, col_el: ET.Element, pk_ids: set) -> Optional[ERColumn]:
        name = self._get_attr(col_el, "name")
        if not name:
            return None

        col_id = col_el.get("id", "")
        raw_type = self._get_attr(col_el, "simpleType") or self._get_attr(col_el, "userType") or "VARCHAR"
        data_type = self.TYPE_MAP.get(raw_type, raw_type.upper().split(".")[-1])

        is_pk = col_id in pk_ids or self._get_attr(col_el, "flags", "").find("PRIMARY") >= 0

        return ERColumn(
            name=name,
            data_type=data_type,
            is_primary_key=is_pk,
            is_nullable=self._get_attr(col_el, "isNotNull") != "1",
            default_value=self._get_attr(col_el, "defaultValue"),
            comment=self._get_attr(col_el, "comment"),
        )

    def _find_relationships(self, root: ET.Element) -> list:
        rels = []
        for el in root.iter("value"):
            if el.get("type") in ("db.mysql.ForeignKey", "db.ForeignKey"):
                rels.append(el)
        return rels

    def _parse_relationship(self, rel_el: ET.Element) -> Optional[ERRelationship]:
        name = self._get_attr(rel_el, "name") or "unnamed_fk"

        # Resolve table references
        owner_id = self._get_attr(rel_el, "owner") or ""
        ref_table_id = self._get_attr(rel_el, "referencedTable") or ""

        from_table = self._table_id_map.get(owner_id, "")
        to_table = self._table_id_map.get(ref_table_id, "")

        if not from_table or not to_table:
            # Try resolving by searching for ref content
            for child in rel_el:
                if child.get("struct-name") == "referencedTable":
                    ref_table_id = child.text or ""
                    to_table = self._table_id_map.get(ref_table_id, "")

        # Column mapping
        from_col, to_col = self._resolve_fk_columns(rel_el)

        # Cardinality
        many = self._get_attr(rel_el, "many") or "n"
        one = self._get_attr(rel_el, "referencedMandatory") or "1"
        cardinality = infer_cardinality(many, one)

        semantic_label = infer_relationship_label(name, from_table, to_table)

        return ERRelationship(
            name=name,
            from_table=from_table,
            to_table=to_table,
            from_column=from_col,
            to_column=to_col,
            cardinality=cardinality,
            semantic_label=semantic_label,
        )

    def _resolve_fk_columns(self, rel_el: ET.Element) -> tuple[str, str]:
        """Extract FK column pair from relationship element"""
        cols_el = None
        ref_cols_el = None
        for child in rel_el:
            tag = child.get("content-struct-name") or child.tag
            key = child.get("key") or ""
            if "columns" in key.lower() and "referenced" not in key.lower():
                cols_el = child
            elif "referencedColumns" in key or "referenced" in key.lower():
                ref_cols_el = child

        from_col = to_col = ""
        if cols_el is not None:
            id_ = cols_el.text or (cols_el[0].text if len(cols_el) > 0 else "")
            from_col = self._column_id_map.get(id_.strip(), ("", ""))[1] if id_ else ""
        if ref_cols_el is not None:
            id_ = ref_cols_el.text or (ref_cols_el[0].text if len(ref_cols_el) > 0 else "")
            to_col = self._column_id_map.get(id_.strip(), ("", ""))[1] if id_ else ""

        return from_col or "id", to_col or "id"

    @staticmethod
    def _get_attr(el: ET.Element, key: str, default: str = "") -> str:
        """Get named child value or attribute"""
        # Try direct attribute
        val = el.get(key)
        if val is not None:
            return val
        # Try child element with matching key
        for child in el:
            if child.get("key") == key:
                return child.text or default
        return default


# ── DDL Parser (future / fallback) ────────────────────────────────────────────

class DDLParser:
    """
    Simple SQL DDL parser for CREATE TABLE statements.
    Used as fallback when .mwb is not available.
    """

    def parse(self, ddl_text: str) -> ERModel:
        model = ERModel(source_format="ddl")
        table_blocks = re.findall(
            r"CREATE\s+TABLE\s+[`\"]?(\w+)[`\"]?\s*\((.*?)\);",
            ddl_text, re.IGNORECASE | re.DOTALL
        )
        for tbl_name, body in table_blocks:
            table = ERTable(name=tbl_name, node_label=to_pascal_case(tbl_name))
            lines = [l.strip() for l in body.split("\n") if l.strip()]
            for line in lines:
                col = self._parse_ddl_column(line)
                if col:
                    table.columns.append(col)
                    if col.is_primary_key:
                        table.primary_keys.append(col.name)
            model.tables.append(table)
        # Extract FOREIGN KEY constraints
        fk_blocks = re.findall(
            r"FOREIGN\s+KEY\s*\([`\"]?(\w+)[`\"]?\)\s+REFERENCES\s+[`\"]?(\w+)[`\"]?\s*\([`\"]?(\w+)[`\"]?\)",
            ddl_text, re.IGNORECASE
        )
        for from_col, to_table, to_col in fk_blocks:
            model.relationships.append(ERRelationship(
                name=f"fk_{from_col}",
                from_table="",  # needs context
                to_table=to_table,
                from_column=from_col,
                to_column=to_col,
                semantic_label=infer_relationship_label(from_col, "", to_table),
            ))
        return model

    def _parse_ddl_column(self, line: str) -> Optional[ERColumn]:
        if any(kw in line.upper() for kw in ("PRIMARY KEY", "FOREIGN KEY", "UNIQUE KEY", "KEY ", "CONSTRAINT", "INDEX")):
            return None
        m = re.match(r"[`\"]?(\w+)[`\"]?\s+(\w+)", line)
        if not m:
            return None
        name, dtype = m.group(1), m.group(2).upper()
        return ERColumn(
            name=name,
            data_type=dtype,
            is_primary_key="PRIMARY KEY" in line.upper(),
            is_nullable="NOT NULL" not in line.upper(),
        )


# ── Factory ───────────────────────────────────────────────────────────────────

class ERParserFactory:
    """Returns the right parser based on format config"""

    @staticmethod
    def get_parser(fmt: str, path: str):
        fmt = fmt.lower()
        if fmt == "mwb":
            return MWBParser(path)
        elif fmt == "ddl":
            return DDLParser()
        else:
            raise NotImplementedError(f"ER format '{fmt}' not yet supported. Supported: mwb, ddl")

    @staticmethod
    def parse(config: dict) -> ERModel:
        fmt = config["er_diagram"]["format"]
        path = config["er_diagram"]["path"]
        parser = ERParserFactory.get_parser(fmt, path)
        if fmt == "ddl":
            return parser.parse(Path(path).read_text())
        return parser.parse()


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, yaml
    logging.basicConfig(level=logging.DEBUG)
    cfg = yaml.safe_load(open(sys.argv[1] if len(sys.argv) > 1 else "config.yaml"))
    model = ERParserFactory.parse(cfg)
    out = json.dumps(
        {
            "tables": [asdict(t) for t in model.tables],
            "relationships": [asdict(r) for r in model.relationships],
        },
        indent=2,
    )
    out_path = Path("mappings/er_model.json")
    out_path.write_text(out)
    print(f"✓ ER model saved → {out_path}")
    print(f"  Tables: {len(model.tables)}, Relationships: {len(model.relationships)}")

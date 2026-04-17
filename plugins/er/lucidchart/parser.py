"""
Lucidchart ER Diagram Parser Plugin for rdb2graph
Parses Lucidchart exports: CSV (Entity Shapes export) or VSDX (Visio XML).

Lucidchart export options:
  1. File → Export → CSV  →  use format: "lucidchart_csv"
  2. File → Export → Visio (.vsdx)  →  use format: "lucidchart_vsdx"  (requires python-pptx or lxml)

Install:
  CSV mode:   No extra packages (stdlib csv only)
  VSDX mode:  pip install lxml>=4.9.0

config.yaml:
    er_diagram:
      path: "./lucidchart_export.csv"
      format: "lucidchart"
"""
import csv
import logging
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from src.er_parser import ERParserBase, ERModel, ERTable, ERColumn, ERRelationship, to_pascal_case
except ImportError:
    import sys, os; sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))
    from er_parser import ERParserBase, ERModel, ERTable, ERColumn, ERRelationship, to_pascal_case  # type: ignore


class LucidchartParser(ERParserBase):
    """
    Parses Lucidchart ER diagrams.
    Auto-detects format from file extension: .csv or .vsdx
    """

    PLUGIN_NAME = "lucidchart"
    FILE_EXTENSIONS = [".csv", ".vsdx"]
    REQUIRED_PACKAGES = []   # lxml optional for vsdx

    def __init__(self, path: str):
        self.path = path
        self.ext = Path(path).suffix.lower()

    def parse(self) -> ERModel:
        logger.info(f"Parsing Lucidchart export ({self.ext}): {self.path}")
        if self.ext == ".csv":
            return self._parse_csv()
        elif self.ext == ".vsdx":
            return self._parse_vsdx()
        else:
            raise ValueError(f"Unsupported Lucidchart export format: {self.ext}. Use .csv or .vsdx")

    # ── CSV mode ──────────────────────────────────────────────────────────────
    # Lucidchart CSV export format:
    # Id, Name, Shape Library, Page ID, Contained By, ...
    # Entity rows:  Shape Library = "Entity Relationship"
    # The CSV has one row per shape; column details embedded in "Text Area X" fields

    def _parse_csv(self) -> ERModel:
        model = ERModel(source_format="lucidchart_csv", source_path=self.path)
        with open(self.path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        entity_rows = [r for r in rows if "entity" in (r.get("Shape Library") or "").lower()
                       or "table" in (r.get("Name") or "").lower()]
        rel_rows = [r for r in rows if r.get("Line Source") or r.get("Line Destination")]

        # Build tables from entity rows
        table_map: dict[str, ERTable] = {}
        for row in entity_rows:
            name = (row.get("Name") or "").strip()
            if not name or name.lower() in ("entity", "table"):
                continue
            table = ERTable(name=name, node_label=to_pascal_case(name))
            # Lucidchart embeds column info in "Text Area 1", "Text Area 2", ...
            for key in sorted(row.keys()):
                if key.startswith("Text Area"):
                    col = self._parse_column_text(row[key])
                    if col:
                        table.columns.append(col)
                        if col.is_primary_key:
                            table.primary_keys.append(col.name)
            table_map[row.get("Id", name)] = table
            model.tables.append(table)

        # Build relationships from edge rows
        id_to_name = {row.get("Id"): (row.get("Name") or "").strip() for row in rows}
        for row in rel_rows:
            src_id = row.get("Line Source")
            tgt_id = row.get("Line Destination")
            if not src_id or not tgt_id:
                continue
            src_tbl = table_map.get(src_id)
            tgt_tbl = table_map.get(tgt_id)
            if not src_tbl or not tgt_tbl:
                continue
            label = f"BELONGS_TO_{to_pascal_case(tgt_tbl.name).upper()}"
            model.relationships.append(ERRelationship(
                name=row.get("Name") or f"rel_{src_tbl.name}_{tgt_tbl.name}",
                from_table=src_tbl.name, to_table=tgt_tbl.name,
                from_column="id", to_column="id",
                cardinality="many-to-one",
                semantic_label=label, direction="OUTGOING",
            ))

        logger.info(f"Lucidchart CSV: {len(model.tables)} tables, {len(model.relationships)} relationships")
        return model

    def _parse_column_text(self, text: str) -> Optional[ERColumn]:
        if not text or not text.strip():
            return None
        text = text.strip()
        is_pk = bool(re.search(r"\bPK\b|\bPrimary\b", text, re.IGNORECASE))
        is_fk = bool(re.search(r"\bFK\b|\bForeign\b", text, re.IGNORECASE))
        type_m = re.search(r"[:\s(]\s*([A-Z][A-Z0-9_]+)", text)
        dtype = type_m.group(1).upper() if type_m else "VARCHAR"
        name_m = re.match(r"([a-zA-Z_]\w*)", text)
        if not name_m:
            return None
        return ERColumn(name=name_m.group(1), data_type=dtype,
                        is_primary_key=is_pk, is_foreign_key=is_fk, is_nullable=not is_pk)

    # ── VSDX mode ─────────────────────────────────────────────────────────────
    # .vsdx is a ZIP containing visio/pages/page1.xml

    def _parse_vsdx(self) -> ERModel:
        model = ERModel(source_format="lucidchart_vsdx", source_path=self.path)
        with zipfile.ZipFile(self.path) as zf:
            page_files = [n for n in zf.namelist() if re.match(r"visio/pages/page\d+\.xml", n)]
            for page_file in page_files:
                xml_content = zf.read(page_file)
                root = ET.fromstring(xml_content)
                self._parse_vsdx_page(root, model)
        logger.info(f"Lucidchart VSDX: {len(model.tables)} tables, {len(model.relationships)} relationships")
        return model

    def _parse_vsdx_page(self, root: ET.Element, model: ERModel):
        # Visio XML: shapes are <Shape> elements; text in <Text> child
        # This is a best-effort parse — Lucidchart VSDX varies by template
        shapes = {}
        for shape in root.iter("Shape"):
            shape_id = shape.get("ID")
            text_el = shape.find(".//{http://schemas.microsoft.com/office/visio/2012/main}Text")
            text = (text_el.text or "").strip() if text_el is not None else ""
            if not text:
                text_el = shape.find(".//Text")
                text = (text_el.text or "").strip() if text_el is not None else ""
            if text:
                shapes[shape_id] = text

        # Heuristic: shapes with multi-line text are likely tables
        for shape_id, text in shapes.items():
            lines = [l.strip() for l in text.splitlines() if l.strip()]
            if len(lines) >= 2:
                table = ERTable(name=lines[0], node_label=to_pascal_case(lines[0]))
                for line in lines[1:]:
                    col = self._parse_column_text(line)
                    if col:
                        table.columns.append(col)
                        if col.is_primary_key:
                            table.primary_keys.append(col.name)
                model.tables.append(table)
